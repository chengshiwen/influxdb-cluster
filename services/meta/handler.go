package meta

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgrijalva/jwt-go/v4"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/influxdata/influxdb/pkg/httputil"
	"github.com/influxdata/influxdb/pkg/jwtutil"
	"github.com/influxdata/influxdb/query"
	internal "github.com/influxdata/influxdb/services/meta/internal"
	"github.com/influxdata/influxdb/uuid"
	"go.uber.org/zap"
)

// AuthenticationMethod defines the type of authentication used.
type AuthenticationMethod int

// Supported authentication methods.
const (
	// Authenticate using basic authentication.
	UserAuthentication AuthenticationMethod = iota

	// Authenticate with jwt.
	BearerAuthentication
)

type RPCClient interface {
	CopyShard(address, host, database, policy string, shardID uint64, since time.Time) error
	RemoveShard(address string, shardID uint64) error
	ListShards(address string) (map[uint64]*ShardOwnerInfo, error)
	JoinCluster(address string, metaServers []string, update bool) (*NodeInfo, error)
	LeaveCluster(address string) error
	RemoveHintedHandoff(address string, nodeID uint64) error
}

// handler represents an HTTP handler for the meta service.
type handler struct {
	config *Config

	logger         *zap.Logger
	clfLogger      *log.Logger
	loggingEnabled bool // Log every HTTP access.
	pprofEnabled   bool
	store          interface {
		bootstrap() error
		afterIndex(index uint64) <-chan struct{}
		index() uint64
		isLeader() bool
		leader() string
		leaderHTTP() string
		snapshot() (*Data, error)
		apply(b []byte) error
		join(addr, raftAddr string) (*NodeInfo, error)
		leave(raftAddr string) error
		remove(addr string) error
		removeData(tcpAddr string) error
		updateData(addr, tcpAddr, oldTCPAddr string) (*NodeInfo, error)
		dataNodeByTCPAddr(tcpAddr string) (*NodeInfo, error)
		copyShard(id, nodeID uint64) error
		removeShard(id, nodeID uint64) error
		truncateShards(delay time.Duration) error
		metaServersHTTP() []string
		otherMetaServersHTTP() []string
		dataServers() []string
		peers() []string
		createUser(name, password string, admin bool) error
		dropUser(name string) error
		updateUser(name, password string) error
		adminUserExists() bool
		authenticate(username, password string) (User, error)
		user(name string) (User, error)
		users() []UserInfo
		status() *MetaNodeStatus
		cluster() *ClusterInfo
		shards() []*ClusterShardInfo
		shard(id uint64) *ClusterShardInfo
	}
	s *Service

	mu      sync.RWMutex
	closing chan struct{}
	leases  *Leases

	announcements Announcements

	client     *httputil.Client
	dataClient *httputil.Client
	rpcClient  RPCClient
}

// newHandler returns a new instance of handler with routes.
func newHandler(c *Config, s *Service) *handler {
	h := &handler{
		s:              s,
		config:         c,
		logger:         zap.NewNop(),
		clfLogger:      log.New(os.Stderr, "[meta-http] ", 0),
		loggingEnabled: c.ClusterTracing,
		closing:        make(chan struct{}),
		leases:         NewLeases(time.Duration(c.LeaseDuration)),
		announcements:  make(Announcements),
		client: httputil.NewClient(httputil.Config{
			AuthEnabled: c.AuthEnabled,
			AuthType:    httputil.AuthTypeJWT,
			Secret:      c.InternalSharedSecret,
			UserAgent:   "InfluxDB Meta Service",
			UseTLS:      c.HTTPSEnabled,
			SkipTLS:     c.HTTPSInsecureTLS,
		}),
		dataClient: httputil.NewClient(httputil.Config{
			UserAgent: "InfluxDB Meta Service",
			UseTLS:    c.DataUseTLS,
			SkipTLS:   c.DataInsecureTLS,
		}),
		rpcClient: s.RPCClient,
	}

	return h
}

// WrapHandler sets the provided routes on the handler.
func (h *handler) WrapHandler(name string, hf http.HandlerFunc) http.Handler {
	var handler http.Handler
	handler = http.HandlerFunc(hf)
	handler = authenticate(handler, h)
	handler = gzipFilter(handler)
	handler = versionHeader(handler, h)
	handler = metaIndexHeader(handler, h)
	handler = requestID(handler)
	if h.loggingEnabled {
		handler = h.logging(handler, name)
	}
	handler = h.recovery(handler, name) // make sure recovery is always last

	return handler
}

// WithLogger sets the logger for the handler.
func (h *handler) WithLogger(log *zap.Logger) {
	h.logger = log.With(zap.String("service", "meta-http"))
}

// ServeHTTP responds to HTTP request to the handler.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		switch r.URL.Path {
		case "/":
			h.WrapHandler("snapshot", h.serveSnapshot).ServeHTTP(w, r)
		case "/ping":
			h.WrapHandler("ping", h.servePing).ServeHTTP(w, r)
		case "/lease":
			h.WrapHandler("lease", h.serveLease).ServeHTTP(w, r)
		case "/peers":
			h.WrapHandler("peers", h.servePeers).ServeHTTP(w, r)
		case "/status":
			h.WrapHandler("status", h.serveStatus).ServeHTTP(w, r)
		case "/show-cluster":
			h.WrapHandler("show-cluster", h.serveShowCluster).ServeHTTP(w, r)
		case "/show-shards":
			h.WrapHandler("show-shards", h.serveShowShards).ServeHTTP(w, r)
		case "/user":
			h.WrapHandler("user", h.serveUser).ServeHTTP(w, r)
		case "/role":
			h.WrapHandler("role", h.serveRole).ServeHTTP(w, r)
		default:
			if strings.HasPrefix(r.URL.Path, "/debug/pprof") && h.config.PprofEnabled {
				h.handleProfiles(w, r)
				return
			}
			http.NotFound(w, r)
		}
	case "POST":
		switch r.URL.Path {
		case "/execute":
			h.WrapHandler("execute", h.serveExec).ServeHTTP(w, r)
		case "/join":
			h.WrapHandler("join", h.serveJoin).ServeHTTP(w, r)
		case "/leave":
			h.WrapHandler("leave", h.serveLeave).ServeHTTP(w, r)
		case "/remove":
			h.WrapHandler("remove", h.serveRemove).ServeHTTP(w, r)
		case "/add-data":
			h.WrapHandler("add-data", h.serveAddData).ServeHTTP(w, r)
		case "/remove-data":
			h.WrapHandler("remove-data", h.serveRemoveData).ServeHTTP(w, r)
		case "/update-data":
			h.WrapHandler("update-data", h.serveUpdateData).ServeHTTP(w, r)
		case "/copy-shard":
			h.WrapHandler("copy-shard", h.serveCopyShard).ServeHTTP(w, r)
		case "/remove-shard":
			h.WrapHandler("remove-shard", h.serveRemoveShard).ServeHTTP(w, r)
		case "/truncate-shards":
			h.WrapHandler("truncate-shards", h.serveTruncateShards).ServeHTTP(w, r)
		case "/announce":
			h.WrapHandler("announce", h.serveAnnounce).ServeHTTP(w, r)
		case "/user":
			h.WrapHandler("user", h.serveUser).ServeHTTP(w, r)
		case "/role":
			h.WrapHandler("role", h.serveRole).ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	default:
		http.Error(w, "", http.StatusBadRequest)
	}
}

func (h *handler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.client.CloseIdleConnections()
	h.dataClient.CloseIdleConnections()

	select {
	case <-h.closing:
		// do nothing here
	default:
		close(h.closing)
	}
	return nil
}

func (h *handler) isClosed() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	select {
	case <-h.closing:
		return true
	default:
		return false
	}
}

// serveExec executes the requested command.
func (h *handler) serveExec(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	// Read the command from the request body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Make sure it's a valid command.
	if err := validateCommand(body); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Apply the command to the store.
	var resp *internal.Response
	if err := h.store.apply(body); err != nil {
		// If we aren't the leader, redirect client to the leader.
		if err == raft.ErrNotLeader {
			l := h.store.leaderHTTP()
			if l == "" {
				// No cluster leader. Client will have to try again later.
				h.httpError(w, "no leader", http.StatusServiceUnavailable)
				return
			}
			l = fmt.Sprintf("%s://%s/execute", h.s.HTTPScheme(), l)
			http.Redirect(w, r, l, http.StatusTemporaryRedirect)
			return
		}

		// Error wasn't a leadership error so pass it back to client.
		resp = &internal.Response{
			OK:    proto.Bool(false),
			Error: proto.String(err.Error()),
		}
	} else {
		// Apply was successful. Return the new store index to the client.
		resp = &internal.Response{
			OK:    proto.Bool(false),
			Index: proto.Uint64(h.store.index()),
		}
	}

	// Marshal the response.
	b, err := proto.Marshal(resp)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send response to client.
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Write(b)
}

func validateCommand(b []byte) error {
	// Ensure command can be deserialized before applying.
	if err := proto.Unmarshal(b, &internal.Command{}); err != nil {
		return fmt.Errorf("unable to unmarshal command: %s", err)
	}
	return nil
}

// serveJoin
func (h *handler) serveJoin(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	addr := r.FormValue("addr")
	if addr == "" {
		h.httpError(w, "addr is required", http.StatusBadRequest)
		return
	}

	ns := &MetaNodeStatus{}
	uri := fmt.Sprintf("%s://%s/status", h.s.HTTPScheme(), addr)
	if err := requestStatus(h.client, uri, ns); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	leader := h.store.leader()
	if ns.Leader != "" && ns.Leader != leader {
		err := fmt.Errorf("meta node at \"%s\" is a member of another cluster: leader=%s peers=%v", addr, ns.Leader, ns.Peers)
		h.httpError(w, err.Error(), http.StatusConflict)
		return
	}

	if leader == "" {
		err := h.store.bootstrap()
		if err == raft.ErrCantBootstrap {
			err = fmt.Errorf("dangled meta node at \"%s\" already has state present, cannot add another meta node", r.Host)
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return
		} else if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = h.store.join(h.s.HTTPAddr(), h.s.RaftAddr())
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	node, err := h.store.join(ns.HTTPAddr, ns.RaftAddr)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/join", h.s.HTTPScheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	} else if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	mn := NewMetaNodeInfo(node)

	// Return the node with newly assigned ID as json
	w.Header().Add("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(mn); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
	}
}

// serveLeave
func (h *handler) serveLeave(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	l := h.store.leaderHTTP()
	if l == "" {
		// No cluster leader. Client will have to try again later.
		h.httpError(w, "no leader", http.StatusServiceUnavailable)
		return
	}

	uri := fmt.Sprintf("%s://%s/remove", h.s.HTTPScheme(), l)
	data := url.Values{"httpAddr": {h.s.HTTPAddr()}}
	if err := requestRemove(h.client, uri, data); err != nil {
		h.httpError(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	if err := h.store.leave(h.s.RaftAddr()); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// serveRemove
func (h *handler) serveRemove(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	httpAddr := r.FormValue("httpAddr")
	if httpAddr == "" {
		h.httpError(w, "httpAddr is required", http.StatusBadRequest)
		return
	}

	err := h.store.remove(httpAddr)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/remove", h.s.HTTPScheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	} else if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	force := r.FormValue("force") == "true"
	tcpAddr := r.FormValue("tcpAddr")
	if force && tcpAddr != "" {
		// Forcibly removes a failed node from the cluster immediately
	}

	w.WriteHeader(http.StatusNoContent)
}

// serveAddData
func (h *handler) serveAddData(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	addr := r.FormValue("addr")
	if addr == "" {
		h.httpError(w, "addr is required", http.StatusBadRequest)
		return
	}

	leader := h.store.leader()
	if leader == "" {
		h.httpError(w, "data node failed to contact valid meta server in list []", http.StatusBadRequest)
		return
	}

	node, err := h.store.dataNodeByTCPAddr(addr)
	if err != nil {
		// Join cluster if node not found
		node, err = h.rpcClient.JoinCluster(addr, h.store.metaServersHTTP(), false)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	dn := NewDataNodeInfo(node)

	// Return the node with newly assigned ID as json
	w.Header().Add("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(dn); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
	}
}

// serveRemoveData
func (h *handler) serveRemoveData(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	addr := r.FormValue("addr")
	if addr == "" {
		h.httpError(w, "addr is required", http.StatusBadRequest)
		return
	}

	node, err := h.store.dataNodeByTCPAddr(addr)
	if err != nil {
		h.httpError(w, fmt.Sprintf("node not found: %s", addr), http.StatusBadRequest)
		return
	}

	err = h.store.removeData(addr)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/remove-data", h.s.HTTPScheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	} else if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, tcpAddr := range h.store.dataServers() {
		if tcpAddr != addr {
			go func(tcpAddr string) {
				h.rpcClient.RemoveHintedHandoff(tcpAddr, node.ID)
			}(tcpAddr)
		}
	}

	force := r.FormValue("force") == "true"
	if force {
		// Forcibly removes a failed node from the cluster immediately
	} else {
		err = h.rpcClient.LeaveCluster(addr)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// serveUpdateData
func (h *handler) serveUpdateData(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	oldAddr := r.FormValue("oldAddr")
	newAddr := r.FormValue("newAddr")
	if oldAddr == "" {
		h.httpError(w, "oldAddr is required", http.StatusBadRequest)
		return
	}
	if newAddr == "" {
		h.httpError(w, "newAddr is required", http.StatusBadRequest)
		return
	}
	if newAddr == oldAddr {
		h.httpError(w, "newAddr and oldAddr are the same", http.StatusBadRequest)
		return
	}

	if _, err := h.store.dataNodeByTCPAddr(oldAddr); err != nil {
		h.httpError(w, fmt.Sprintf("no data node with bind address %s exists", oldAddr), http.StatusBadRequest)
		return
	}

	ns := &DataNodeStatus{}
	uri := fmt.Sprintf("%s://%s/status", h.dataHTTPScheme(), newAddr)
	if err := requestStatus(h.dataClient, uri, ns); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if ns.NodeStatus == NodeStatusJoined {
		_, err := h.store.dataNodeByTCPAddr(newAddr)
		if err != nil {
			h.httpError(w, "already joined to cluster", http.StatusInternalServerError)
		} else {
			err = fmt.Errorf("cannot update data node %s to %s; %s is already part of cluster", oldAddr, newAddr, newAddr)
			h.httpError(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	node, err := h.store.updateData(ns.HTTPAddr, ns.TCPAddr, oldAddr)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/update-data", h.s.HTTPScheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	} else if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	node, err = h.rpcClient.JoinCluster(newAddr, h.store.metaServersHTTP(), true)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	dn := NewDataNodeInfo(node)

	// Return the node with newly assigned ID as json
	w.Header().Add("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(dn); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
	}
}

// serveSnapshot is a long polling http connection to server cache updates
func (h *handler) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusInternalServerError)
		return
	}

	// get the current index that client has
	index, err := strconv.ParseUint(r.URL.Query().Get("index"), 10, 64)
	if err != nil {
		http.Error(w, "error parsing index", http.StatusBadRequest)
	}

	select {
	case <-h.store.afterIndex(index):
		// Send updated snapshot to client.
		ss, err := h.store.snapshot()
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		b, err := ss.MarshalBinary()
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(b)
		return
	case <-w.(http.CloseNotifier).CloseNotify():
		// Client closed the connection so we're done.
		return
	case <-h.closing:
		h.httpError(w, "server closed", http.StatusInternalServerError)
		return
	}
}

// servePing will return if the server is up, or if specified will check the status
// of the other metaservers as well
func (h *handler) servePing(w http.ResponseWriter, r *http.Request) {
	// if they're not asking to check all servers, just return who we think
	// the leader is
	if r.URL.Query().Get("all") == "" {
		w.Write([]byte(h.store.leader()))
		return
	}

	leader := h.store.leader()
	healthy := true
	for _, n := range h.store.otherMetaServersHTTP() {
		url := fmt.Sprintf("%s://%s/ping", h.s.HTTPScheme(), n)

		resp, err := h.client.Get(url)
		if err != nil {
			healthy = false
			break
		}

		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			healthy = false
			break
		}

		if leader != string(b) {
			healthy = false
			break
		}
	}

	if healthy {
		w.Write([]byte(h.store.leader()))
		return
	}

	h.httpError(w, "one or more metaservers not up", http.StatusInternalServerError)
}

// servePeers
func (h *handler) servePeers(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(h.store.peers()); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
	}
}

// serveStatus
func (h *handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(h.store.status()); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
	}
}

// serveShowCluster
func (h *handler) serveShowCluster(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	cluster := h.store.cluster()
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, dn := range cluster.Data {
		if ann, ok := h.announcements[dn.TCPAddr]; ok {
			dn.HTTPScheme, dn.Version = ann.HTTPScheme, ann.Version
		}
	}
	for _, mn := range cluster.Meta {
		if ann, ok := h.announcements[mn.TCPAddr]; ok {
			mn.HTTPScheme, mn.Version = ann.HTTPScheme, ann.Version
		} else if mn.TCPAddr == h.s.RaftAddr() {
			mn.HTTPScheme, mn.Version = h.s.HTTPScheme(), h.s.Version
		}
	}
	if err := json.NewEncoder(w).Encode(cluster); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
	}
}

// serveShowShards
func (h *handler) serveShowShards(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	shardInfos := h.store.shards()
	verbose := r.URL.Query().Get("verbose") == "true"
	if verbose {
		var wg sync.WaitGroup
		for _, tcpAddr := range h.store.dataServers() {
			wg.Add(1)
			go func(tcpAddr string) {
				defer wg.Done()
				shards, err := h.rpcClient.ListShards(tcpAddr)
				if err != nil || len(shards) == 0 {
					return
				}
				for _, si := range shardInfos {
					if owner, ok := shards[si.ID]; ok {
						for _, oi := range si.Owners {
							if oi.ID == owner.ID {
								oi.State = owner.State
								oi.LastModified = owner.LastModified
								oi.Size = owner.Size
								oi.Err = owner.Err
								break
							}
						}
					}
				}
			}(tcpAddr)
		}
		wg.Wait()

		for _, si := range shardInfos {
			for _, oi := range si.Owners {
				if oi.State == "" && oi.Err == "" {
					oi.Err = "not found"
				}
			}
		}
	}

	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(shardInfos); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
	}
}

// serveCopyShard
func (h *handler) serveCopyShard(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	src := r.FormValue("src")
	dest := r.FormValue("dest")
	if src == "" {
		h.httpError(w, "'src' is a required parameter", http.StatusBadRequest)
		return
	}
	if dest == "" {
		h.httpError(w, "'dest' is a required parameter", http.StatusBadRequest)
		return
	}
	if dest == src {
		h.httpError(w, "dest and src are the same", http.StatusBadRequest)
		return
	}

	srcNode, err := h.store.dataNodeByTCPAddr(src)
	if err != nil {
		h.httpError(w, fmt.Sprintf("unable to find node for \"%s\"", src), http.StatusBadRequest)
		return
	}
	destNode, err := h.store.dataNodeByTCPAddr(dest)
	if err != nil {
		h.httpError(w, fmt.Sprintf("unable to find node for \"%s\"", dest), http.StatusBadRequest)
		return
	}

	shard := r.FormValue("shard")
	shardID, err := strconv.ParseUint(shard, 10, 64)
	if err != nil {
		h.httpError(w, fmt.Sprintf("error converting shard to int: %s", shard), http.StatusBadRequest)
		return
	}
	if shardID <= 0 {
		h.httpError(w, "invalid parameter value for 'shard': shard id must be greater than 0", http.StatusBadRequest)
		return
	}
	si := h.store.shard(shardID)
	if si == nil {
		h.httpError(w, fmt.Sprintf("shard not found for id: %d", shardID), http.StatusBadRequest)
		return
	}
	isOwner := false
	for _, owner := range si.Owners {
		if owner.ID == srcNode.ID {
			isOwner = true
			break
		}
	}
	if !isOwner {
		h.httpError(w, fmt.Sprintf("\"%s\" is not an owner of shard %d", src, shardID), http.StatusBadRequest)
		return
	}

	if !h.store.isLeader() {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/copy-shard", h.s.HTTPScheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	}

	err = h.rpcClient.CopyShard(dest, src, si.Database, si.RetentionPolicy, shardID, time.Time{})
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = h.store.copyShard(shardID, destNode.ID)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// serveRemoveShard
func (h *handler) serveRemoveShard(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	src := r.FormValue("src")
	if src == "" {
		h.httpError(w, "'src' is a required parameter", http.StatusBadRequest)
		return
	}

	srcNode, err := h.store.dataNodeByTCPAddr(src)
	if err != nil {
		h.httpError(w, fmt.Sprintf("unable to find node for \"%s\"", src), http.StatusBadRequest)
		return
	}

	shard := r.FormValue("shard")
	shardID, err := strconv.ParseUint(shard, 10, 64)
	if err != nil {
		h.httpError(w, fmt.Sprintf("error converting shard to int: %s", shard), http.StatusBadRequest)
		return
	}
	if shardID <= 0 {
		h.httpError(w, "invalid parameter value for 'shard': shard id must be greater than 0", http.StatusBadRequest)
		return
	}
	si := h.store.shard(shardID)
	if si == nil {
		h.httpError(w, fmt.Sprintf("shard not found for id: %d", shardID), http.StatusBadRequest)
		return
	}

	if !h.store.isLeader() {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/remove-shard", h.s.HTTPScheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	}

	err = h.rpcClient.RemoveShard(src, shardID)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = h.store.removeShard(shardID, srcNode.ID)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// serveTruncateShards
func (h *handler) serveTruncateShards(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	delay, err := time.ParseDuration(r.FormValue("delay"))
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.store.truncateShards(delay)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/truncate-shards", h.s.HTTPScheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	} else if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// serveLease
func (h *handler) serveLease(w http.ResponseWriter, r *http.Request) {
	var name, nodeIDStr string
	q := r.URL.Query()

	// Get the requested lease name.
	name = q.Get("name")
	if name == "" {
		http.Error(w, "lease name required", http.StatusBadRequest)
		return
	}

	// Get the ID of the requesting node.
	nodeIDStr = q.Get("nodeid")
	if nodeIDStr == "" {
		http.Error(w, "node ID required", http.StatusBadRequest)
		return
	}

	// Redirect to leader if necessary.
	leader := h.store.leaderHTTP()
	if leader != h.s.HTTPAddr() {
		if leader == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		leader = fmt.Sprintf("%s://%s/lease?%s", h.s.HTTPScheme(), leader, q.Encode())
		http.Redirect(w, r, leader, http.StatusTemporaryRedirect)
		return
	}

	// Convert node ID to an int.
	nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid node ID", http.StatusBadRequest)
		return
	}

	// Try to acquire the requested lease.
	// Always returns a lease. err determins if we own it.
	l, err := h.leases.Acquire(name, nodeID)
	// Marshal the lease to JSON.
	b, e := json.Marshal(l)
	if e != nil {
		h.httpError(w, e.Error(), http.StatusInternalServerError)
		return
	}
	// Write HTTP status.
	if err != nil {
		// Another node owns the lease.
		w.WriteHeader(http.StatusConflict)
	} else {
		// Lease successfully acquired.
		w.WriteHeader(http.StatusOK)
	}
	// Write the lease data.
	w.Header().Add("Content-Type", "application/json")
	w.Write(b)
	return
}

// serveAnnounce
func (h *handler) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	announcement := &Announcement{}
	if err := json.NewDecoder(r.Body).Decode(announcement); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	context := announcement.Context
	announcement.Context = nil
	announcement.Time = time.Now()
	if announcement.TCPAddr != h.s.RaftAddr() {
		h.mu.Lock()
		h.announcements[announcement.TCPAddr] = announcement
		h.mu.Unlock()
	}

	if context != nil {
		if message, ok := context["gossip"]; ok {
			var announcements Announcements
			if err := announcements.UnmarshalBinary(message); err == nil {
				for _, ann := range announcements {
					if ann.TCPAddr != h.s.RaftAddr() {
						h.mu.Lock()
						h.announcements[ann.TCPAddr] = ann
						h.mu.Unlock()
					}
				}
			}
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// announce gossips its known announcements.
func (h *handler) announce() {
	ticker := time.NewTicker(time.Duration(h.config.GossipFrequency))
	defer ticker.Stop()
	expiry := time.NewTicker(time.Second)
	defer expiry.Stop()
	for {
		select {
		case <-h.closing:
			return

		case <-ticker.C:
			metaServers := h.store.otherMetaServersHTTP()
			if len(metaServers) == 0 {
				continue
			}
			var context Context
			h.mu.RLock()
			if len(h.announcements) > 0 {
				message, _ := h.announcements.MarshalBinary()
				context = Context{"gossip": message}
			}
			announcement := &Announcement{
				TCPAddr:    h.s.RaftAddr(),
				HTTPAddr:   h.s.HTTPAddr(),
				HTTPScheme: h.s.HTTPScheme(),
				Time:       time.Time{},
				NodeType:   NodeTypeMeta,
				Status:     "",
				Context:    context,
				Version:    h.s.Version,
			}
			data, _ := json.Marshal(announcement)
			h.mu.RUnlock()
			for _, i := range rand.Perm(len(metaServers)) {
				uri := fmt.Sprintf("%s://%s/announce", h.s.HTTPScheme(), metaServers[i])
				resp, err := h.client.PostJSON(uri, bytes.NewBuffer(data))
				if err != nil {
					continue
				}
				resp.Body.Close()
				break
			}

		case <-expiry.C:
			h.mu.Lock()
			for tcpAddr, ann := range h.announcements {
				if ann.Time.Add(time.Duration(h.config.AnnouncementExpiration)).Before(time.Now()) {
					delete(h.announcements, tcpAddr)
				}
			}
			h.mu.Unlock()
		}
	}
}

// serveUser
func (h *handler) serveUser(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	if r.Method == http.MethodGet {
		var users []*UserPrivilege
		name := r.URL.Query().Get("name")
		if name != "" {
			user, err := h.store.user(name)
			if err != nil || user == nil {
				h.httpError(w, ErrUserNotFound.Error(), http.StatusNotFound)
				return
			}
			ui := user.(*UserInfo)
			users = append(users, &UserPrivilege{Name: ui.Name, Hash: ui.Hash})
		} else {
			for _, ui := range h.store.users() {
				users = append(users, &UserPrivilege{Name: ui.Name, Hash: ui.Hash})
			}
		}
		userPrivileges := &UserPrivileges{Users: users}

		w.Header().Add("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(userPrivileges); err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	op := &UserOperation{}
	if err := json.NewDecoder(r.Body).Decode(op); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch op.Action {
	case "create", "delete", "change-password", "add-permissions", "remove-permissions":
		// it's valid
	default:
		h.httpError(w, fmt.Sprintf("invalid action: %s", op.Action), http.StatusBadRequest)
		return
	}
	if op.User == nil {
		h.httpError(w, "invalid user", http.StatusBadRequest)
		return
	}
	if op.User.Name == "" {
		h.httpError(w, ErrUsernameRequired.Error(), http.StatusBadRequest)
		return
	}

	switch op.Action {
	case "create":
		if op.User.Password == "" {
			h.httpError(w, ErrPasswordRequired.Error(), http.StatusBadRequest)
			return
		}
		if user, _ := h.store.user(op.User.Name); user != nil {
			h.httpError(w, ErrUserExists.Error(), http.StatusBadRequest)
			return
		}
	case "delete":
		if user, err := h.store.user(op.User.Name); err != nil || user == nil {
			h.httpError(w, ErrUserNotFound.Error(), http.StatusBadRequest)
			return
		}
	case "change-password":
		if user, err := h.store.user(op.User.Name); err != nil || user == nil {
			h.httpError(w, ErrUserNotFound.Error(), http.StatusBadRequest)
			return
		}
		if op.User.Password == "" {
			h.httpError(w, ErrPasswordRequired.Error(), http.StatusBadRequest)
			return
		}
	case "add-permissions":
		// ignored
	case "remove-permissions":
		// ignored
	}

	// Redirect to leader if necessary.
	leader := h.store.leaderHTTP()
	if leader != h.s.HTTPAddr() {
		if leader == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		leader = fmt.Sprintf("%s://%s/user", h.s.HTTPScheme(), leader)
		http.Redirect(w, r, leader, http.StatusTemporaryRedirect)
		return
	}

	switch op.Action {
	case "create":
		if err := h.store.createUser(op.User.Name, op.User.Password, true); err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case "delete":
		if err := h.store.dropUser(op.User.Name); err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case "change-password":
		if err := h.store.updateUser(op.User.Name, op.User.Password); err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case "add-permissions":
		// ignored
	case "remove-permissions":
		// ignored
	}

	w.WriteHeader(http.StatusOK)
}

// serveRole
func (h *handler) serveRole(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	if r.Method == http.MethodGet {
		var roles []*RolePrivilege
		name := r.URL.Query().Get("name")
		if name != "" {
			// role, err := h.store.role(name)
			// if err != nil || role == nil {
			// 	h.httpError(w, ErrRoleNotFound.Error(), http.StatusNotFound)
			// 	return
			// }
			// ri := role.(*RoleInfo)
			// roles = append(roles, &RolePrivilege{Name: ri.Name})
		} else {
			// for _, ri := range h.store.roles() {
			// 	roles = append(roles, &RolePrivilege{Name: ri.Name})
			// }
		}
		rolePrivileges := &RolePrivileges{Roles: roles}

		w.Header().Add("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(rolePrivileges); err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	op := &RoleOperation{}
	if err := json.NewDecoder(r.Body).Decode(op); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch op.Action {
	case "create", "delete", "add-users", "remove-users", "add-permissions", "remove-permissions":
		// it's valid
	default:
		h.httpError(w, fmt.Sprintf("invalid action: %s", op.Action), http.StatusBadRequest)
		return
	}
	if op.Role == nil {
		h.httpError(w, "invalid role", http.StatusBadRequest)
		return
	}
	if op.Role.Name == "" {
		h.httpError(w, ErrRoleNameRequired.Error(), http.StatusBadRequest)
		return
	}

	switch op.Action {
	case "create":
		// if role, _ := h.store.role(op.Role.Name); role != nil {
		// 	h.httpError(w, ErrRoleExists.Error(), http.StatusBadRequest)
		// 	return
		// }
	case "delete":
		// if role, err := h.store.role(op.Role.Name); err != nil || role == nil {
		// 	h.httpError(w, ErrRoleNotFound.Error(), http.StatusBadRequest)
		// 	return
		// }
	case "add-users":
		// ignored
	case "remove-users":
		// ignored
	case "add-permissions":
		// ignored
	case "remove-permissions":
		// ignored
	}

	// Redirect to leader if necessary.
	leader := h.store.leaderHTTP()
	if leader != h.s.HTTPAddr() {
		if leader == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		leader = fmt.Sprintf("%s://%s/role", h.s.HTTPScheme(), leader)
		http.Redirect(w, r, leader, http.StatusTemporaryRedirect)
		return
	}

	switch op.Action {
	case "create":
		// ignored
	case "delete":
		// ignored
	case "add-users":
		// ignored
	case "remove-users":
		// ignored
	case "add-permissions":
		// ignored
	case "remove-permissions":
		// ignored
	}

	w.WriteHeader(http.StatusOK)
}

// Filters and filter helpers

type credentials struct {
	Method   AuthenticationMethod
	Username string
	Password string
	Token    string
}

func parseToken(token string) (user, pass string, ok bool) {
	s := strings.IndexByte(token, ':')
	if s < 0 {
		return
	}
	return token[:s], token[s+1:], true
}

// parseCredentials parses a request and returns the authentication credentials.
// The credentials may be present as URL query params, or as a Basic
// Authentication header.
// As params: http://127.0.0.1/query?u=username&p=password
// As basic auth: http://username:password@127.0.0.1
// As Bearer token in Authorization header: Bearer <JWT_TOKEN_BLOB>
// As Token in Authorization header: Token <username:password>
func parseCredentials(r *http.Request) (*credentials, error) {
	q := r.URL.Query()

	// Check for username and password in URL params.
	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return &credentials{
			Method:   UserAuthentication,
			Username: u,
			Password: p,
		}, nil
	}

	// Check for the HTTP Authorization header.
	if s := r.Header.Get("Authorization"); s != "" {
		// Check for Bearer token.
		strs := strings.Split(s, " ")
		if len(strs) == 2 {
			switch strs[0] {
			case "Bearer":
				return &credentials{
					Method: BearerAuthentication,
					Token:  strs[1],
				}, nil
			case "Token":
				if u, p, ok := parseToken(strs[1]); ok {
					return &credentials{
						Method:   UserAuthentication,
						Username: u,
						Password: p,
					}, nil
				}
			}
		}

		// Check for basic auth.
		if u, p, ok := r.BasicAuth(); ok {
			return &credentials{
				Method:   UserAuthentication,
				Username: u,
				Password: p,
			}, nil
		}
	}

	return nil, fmt.Errorf("unable to parse authentication credentials")
}

// authenticate wraps a handler and ensures that if user credentials are passed in
// an attempt is made to authenticate that user. If authentication fails, an error is returned.
func authenticate(inner http.Handler, h *handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return early if we are not authenticating
		if !h.config.AuthEnabled {
			inner.ServeHTTP(w, r)
			return
		}
		if !h.store.adminUserExists() {
			h.httpError(w, "must create admin user first", http.StatusBadRequest)
			return
		}

		creds, err := parseCredentials(r)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusUnauthorized)
			return
		}

		switch creds.Method {
		case UserAuthentication:
			if creds.Username == "" {
				h.httpError(w, "username required", http.StatusUnauthorized)
				return
			}

			_, err = h.store.authenticate(creds.Username, creds.Password)
			if err != nil {
				h.httpError(w, "authorization failed", http.StatusUnauthorized)
				return
			}
		case BearerAuthentication:
			p := jwt.NewParser()
			token, parts, err := p.ParseUnverified(creds.Token, jwt.MapClaims{})
			if err != nil {
				h.httpError(w, err.Error(), http.StatusUnauthorized)
				return
			}

			// Get the username from the token.
			claims := token.Claims.(jwt.MapClaims)
			username, ok := claims["username"].(string)
			if !ok {
				h.httpError(w, "username in token must be a string", http.StatusUnauthorized)
				return
			} else if username == "" {
				if err := jwtutil.VerifyToken(token, parts, h.config.InternalSharedSecret); err != nil {
					h.httpError(w, jwt.ErrSignatureInvalid.Error(), http.StatusUnauthorized)
					return
				}
				inner.ServeHTTP(w, r)
				return
			}

			if h.config.SharedSecret == "" {
				h.httpError(w, "bearer auth disabled", http.StatusUnauthorized)
				return
			}

			if err := jwtutil.VerifyToken(token, parts, h.config.SharedSecret); err != nil {
				h.httpError(w, jwt.ErrSignatureInvalid.Error(), http.StatusUnauthorized)
				return
			}

			// Lookup user in the metastore.
			if user, err := h.store.user(username); err != nil {
				h.httpError(w, err.Error(), http.StatusUnauthorized)
				return
			} else if user == nil {
				h.httpError(w, ErrUserNotFound.Error(), http.StatusUnauthorized)
				return
			}
		default:
			h.httpError(w, "unsupported authentication", http.StatusUnauthorized)
		}

		inner.ServeHTTP(w, r)
	})
}

// versionHeader takes an HTTP handler and returns an HTTP handler
// and adds the X-Influxdb-Version header to outgoing responses.
func versionHeader(inner http.Handler, h *handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Influxdb-Version", h.s.Version)
		inner.ServeHTTP(w, r)
	})
}

func metaIndexHeader(inner http.Handler, h *handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Influxdb-Metaindex", fmt.Sprintf("%d", h.store.index()))
		inner.ServeHTTP(w, r)
	})
}

func requestID(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// X-Request-Id takes priority.
		rid := r.Header.Get("X-Request-Id")

		// If X-Request-Id is empty, then check Request-Id
		if rid == "" {
			rid = r.Header.Get("Request-Id")
		}

		// If Request-Id is empty then generate a v1 UUID.
		if rid == "" {
			rid = uuid.TimeUUID().String()
		}

		// We read Request-Id in other handler code so we'll use that naming
		// convention from this point in the request cycle.
		r.Header.Set("Request-Id", rid)

		// Set the request ID on the response headers.
		// X-Request-Id is the most common name for a request ID header.
		w.Header().Set("X-Request-Id", rid)

		// We will also set Request-Id for backwards compatibility with previous
		// versions of InfluxDB.
		w.Header().Set("Request-Id", rid)

		inner.ServeHTTP(w, r)
	})
}

func (h *handler) logging(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)

		// Log server errors.
		if l.Status()/100 == 5 {
			h.clfLogger.Println(buildLogLine(l, r, start))
			errStr := l.Header().Get("X-InfluxDB-Error")
			if errStr != "" {
				h.logger.Error(fmt.Sprintf("[%d] - %q", l.Status(), errStr))
			}
		}
	})
}

// if the env var is set, and the value is truthy, then we will *not*
// recover from a panic.
var willCrash bool

func init() {
	var err error
	if willCrash, err = strconv.ParseBool(os.Getenv(query.PanicCrashEnv)); err != nil {
		willCrash = false
	}
}

func (h *handler) recovery(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}

		defer func() {
			if err := recover(); err != nil {
				logLine := buildLogLine(l, r, start)
				logLine = fmt.Sprintf("%s [panic:%s] %s", logLine, err, debug.Stack())
				h.clfLogger.Println(logLine)
				http.Error(w, http.StatusText(http.StatusInternalServerError), 500)

				if willCrash {
					h.clfLogger.Println("\n\n=====\nAll goroutines now follow:")
					buf := debug.Stack()
					h.clfLogger.Printf("%s\n", buf)
					os.Exit(1) // If we panic then the Go server will recover.
				}
			}
		}()

		inner.ServeHTTP(l, r)
	})
}

func (h *handler) dataHTTPScheme() string {
	if h.config.DataUseTLS {
		return "https"
	}
	return "http"
}

// httpError writes an error to the client in a standard format.
func (h *handler) httpError(w http.ResponseWriter, errmsg string, code int) {
	if code == http.StatusUnauthorized {
		// If an unauthorized header will be sent back, add a WWW-Authenticate header
		// as an authorization challenge.
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", "InfluxDB"))
	} else if code/100 != 2 {
		sz := math.Min(float64(len(errmsg)), 1024.0)
		w.Header().Set("X-InfluxDB-Error", errmsg[:int(sz)])
	}

	er := &ErrorResponse{Err: errmsg}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(code)
	b, _ := json.Marshal(er)
	w.Write(b)
}

func requestStatus(client *httputil.Client, uri string, v interface{}) error {
	resp, err := client.Get(uri)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return DecodeErrorResponse(resp.Body)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func requestRemove(client *httputil.Client, uri string, data url.Values) error {
	resp, err := client.PostForm(uri, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return DecodeErrorResponse(resp.Body)
	}
	return nil
}

// ErrorResponse represents a error response.
type ErrorResponse struct {
	Err string `json:"error"`
}

func DecodeErrorResponse(body io.ReadCloser) error {
	er := &ErrorResponse{}
	if err := json.NewDecoder(body).Decode(er); err != nil {
		return err
	}
	return errors.New(er.Err)
}
