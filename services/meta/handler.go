package meta

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/influxdata/influxdb/coordinator/rpc"
	internal "github.com/influxdata/influxdb/services/meta/internal"
	"github.com/influxdata/influxdb/uuid"
	"go.uber.org/zap"
)

// handler represents an HTTP handler for the meta service.
type handler struct {
	config *Config

	logger         *zap.Logger
	loggingEnabled bool // Log every HTTP access.
	pprofEnabled   bool
	store          interface {
		bootstrapCluster() error
		afterIndex(index uint64) <-chan struct{}
		index() uint64
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
		metaServersHTTP() []string
		otherMetaServersHTTP() []string
		peers() []string
		status() *MetaNodeStatus
		cluster() *ClusterInfo
	}
	s *Service

	mu      sync.RWMutex
	closing chan struct{}
	leases  *Leases
}

// newHandler returns a new instance of handler with routes.
func newHandler(c *Config, s *Service) *handler {
	h := &handler{
		s:              s,
		config:         c,
		logger:         zap.NewNop(),
		loggingEnabled: c.ClusterTracing,
		closing:        make(chan struct{}),
		leases:         NewLeases(time.Duration(c.LeaseDuration)),
	}

	return h
}

// WrapHandler sets the provided routes on the handler.
func (h *handler) WrapHandler(name string, hf http.HandlerFunc) http.Handler {
	var handler http.Handler
	handler = http.HandlerFunc(hf)
	handler = gzipFilter(handler)
	handler = versionHeader(handler, h)
	handler = requestID(handler)
	if h.loggingEnabled {
		handler = logging(handler, name, h.logger)
	}
	handler = recovery(handler, name, h.logger) // make sure recovery is always last

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
		default:
			h.WrapHandler("snapshot", h.serveSnapshot).ServeHTTP(w, r)
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
		}
	default:
		http.Error(w, "", http.StatusBadRequest)
	}
}

func (h *handler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
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
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	// Read the command from the request body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	// Make sure it's a valid command.
	if err := validateCommand(body); err != nil {
		h.httpError(err, w, http.StatusBadRequest)
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
				h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
				return
			}
			l = fmt.Sprintf("%s://%s/execute", h.scheme(), l)
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
		h.httpError(err, w, http.StatusInternalServerError)
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

func (h *handler) serveJoin(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	addr := r.FormValue("addr")
	if addr == "" {
		h.httpError(errors.New("addr is empty"), w, http.StatusBadRequest)
		return
	}
	uri := fmt.Sprintf("%s://%s/status", h.scheme(), addr)
	resp, err := http.Get(uri)
	if err != nil {
		h.httpError(err, w, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = DecodeErrorResponse(resp.Body)
		h.httpError(err, w, resp.StatusCode)
		return
	}

	ns := &MetaNodeStatus{}
	if err = json.NewDecoder(resp.Body).Decode(ns); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	leader := h.store.leader()
	if ns.Leader != "" && ns.Leader != leader {
		err = fmt.Errorf("meta node at \"%s\" is a member of another cluster: leader=%s peers=%v", addr, ns.Leader, ns.Peers)
		h.httpError(err, w, http.StatusConflict)
		return
	}

	if leader == "" {
		err = h.store.bootstrapCluster()
		if err == raft.ErrCantBootstrap {
			err = fmt.Errorf("dangled meta node at \"%s\" already has state present, cannot add another meta node", r.Host)
			h.httpError(err, w, http.StatusBadRequest)
			return
		} else if err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}
		httpAddr := h.s.remoteAddr(h.s.httpAddr)
		raftAddr := h.s.remoteAddr(h.s.raftAddr)
		_, err = h.store.join(httpAddr, raftAddr)
		if err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}
	}

	node, err := h.store.join(ns.HTTPAddr, ns.RaftAddr)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/join", h.scheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	} else if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}
	mn := NewMetaNodeInfo(node)

	// Return the node with newly assigned ID as json
	w.Header().Add("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(mn); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *handler) serveLeave(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	l := h.store.leaderHTTP()
	if l == "" {
		// No cluster leader. Client will have to try again later.
		h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
		return
	}
	httpAddr := h.s.remoteAddr(h.s.httpAddr)
	uri := fmt.Sprintf("%s://%s/remove", h.scheme(), l)
	data := url.Values{"httpAddr": {httpAddr}}
	resp, err := http.PostForm(uri, data)
	if err != nil {
		h.httpError(err, w, http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		err = DecodeErrorResponse(resp.Body)
		h.httpError(err, w, resp.StatusCode)
		return
	}

	raftAddr := h.s.remoteAddr(h.s.raftAddr)
	err = h.store.leave(raftAddr)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) serveRemove(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	httpAddr := r.FormValue("httpAddr")
	if httpAddr == "" {
		h.httpError(errors.New("httpAddr is empty"), w, http.StatusBadRequest)
		return
	}
	err := h.store.remove(httpAddr)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/remove", h.scheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	} else if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	force := r.FormValue("force") == "true"
	tcpAddr := r.FormValue("tcpAddr")
	if force && tcpAddr != "" {
		// Forcibly removes a failed node from the cluster immediately
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) serveAddData(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	addr := r.FormValue("addr")
	if addr == "" {
		h.httpError(errors.New("addr is empty"), w, http.StatusBadRequest)
		return
	}

	leader := h.store.leader()
	if leader == "" {
		h.httpError(errors.New("data node failed to contact valid meta server in list []"), w, http.StatusBadRequest)
		return
	}

	node, err := h.store.dataNodeByTCPAddr(addr)
	if err != nil {
		// node not found
		c := rpc.NewClient(addr)
		err = c.JoinCluster(h.store.metaServersHTTP(), false)
		if err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}

		tries, maxTries := 0, 100
		node, err = h.store.dataNodeByTCPAddr(addr)
		for err != nil && tries < maxTries {
			tries += 1
			time.Sleep(100 * time.Millisecond)
			node, err = h.store.dataNodeByTCPAddr(addr)
		}
		if err != nil {
			h.httpError(fmt.Errorf("%s, %d retries", err, maxTries), w, http.StatusInternalServerError)
			return
		}
	}
	dn := NewDataNodeInfo(node)

	// Return the node with newly assigned ID as json
	w.Header().Add("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(dn); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *handler) serveRemoveData(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	addr := r.FormValue("addr")
	if addr == "" {
		h.httpError(errors.New("addr is empty"), w, http.StatusBadRequest)
		return
	}

	if !h.containDataNode(addr) {
		h.httpError(fmt.Errorf("node not found: %s", addr), w, http.StatusBadRequest)
		return
	}

	err := h.store.removeData(addr)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/remove-data", h.scheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	} else if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	force := r.FormValue("force") == "true"
	if force {
		// Forcibly removes a failed node from the cluster immediately
	} else {
		c := rpc.NewClient(addr)
		err = c.LeaveCluster()
		if err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) serveUpdateData(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	oldAddr := r.FormValue("oldAddr")
	newAddr := r.FormValue("newAddr")
	if oldAddr == "" || newAddr == "" {
		h.httpError(errors.New("addr is empty"), w, http.StatusBadRequest)
		return
	}
	if newAddr == oldAddr {
		h.httpError(errors.New("newAddr is same as oldAddr"), w, http.StatusBadRequest)
		return
	}

	if !h.containDataNode(oldAddr) {
		h.httpError(fmt.Errorf("no data node with bind address %s exists", oldAddr), w, http.StatusBadRequest)
		return
	}

	uri := fmt.Sprintf("%s://%s/status", h.dataScheme(), newAddr)
	resp, err := http.Get(uri)
	if err != nil {
		h.httpError(err, w, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = DecodeErrorResponse(resp.Body)
		h.httpError(err, w, resp.StatusCode)
		return
	}

	ns := &DataNodeStatus{}
	if err = json.NewDecoder(resp.Body).Decode(ns); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	if ns.NodeStatus == NodeStatusJoined {
		_, err = h.store.dataNodeByTCPAddr(newAddr)
		if err != nil {
			h.httpError(errors.New("already joined to cluster"), w, http.StatusInternalServerError)
		} else {
			err = fmt.Errorf("cannot update data node %s to %s; %s is already part of cluster", oldAddr, newAddr, newAddr)
			h.httpError(err, w, http.StatusBadRequest)
		}
		return
	}

	node, err := h.store.updateData(ns.HTTPAddr, ns.TCPAddr, oldAddr)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
			return
		}
		l = fmt.Sprintf("%s://%s/update-data", h.scheme(), l)
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	} else if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	c := rpc.NewClient(newAddr)
	err = c.JoinCluster(h.store.metaServersHTTP(), true)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	dn := NewDataNodeInfo(node)

	// Return the node with newly assigned ID as json
	w.Header().Add("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(dn); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *handler) containDataNode(tcpAddr string) bool {
	leader := h.store.leader()
	if leader == "" {
		return false
	}
	_, err := h.store.dataNodeByTCPAddr(tcpAddr)
	return err == nil
}

// serveSnapshot is a long polling http connection to server cache updates
func (h *handler) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusInternalServerError)
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
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}
		b, err := ss.MarshalBinary()
		if err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}
		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(b)
		return
	case <-w.(http.CloseNotifier).CloseNotify():
		// Client closed the connection so we're done.
		return
	case <-h.closing:
		h.httpError(fmt.Errorf("server closed"), w, http.StatusInternalServerError)
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
		url := fmt.Sprintf("%s://%s/ping", h.scheme(), n)

		resp, err := http.Get(url)
		if err != nil {
			healthy = false
			break
		}

		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
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

	h.httpError(fmt.Errorf("one or more metaservers not up"), w, http.StatusInternalServerError)
}

func (h *handler) servePeers(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(h.store.peers()); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(h.store.status()); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *handler) serveShowCluster(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(h.store.cluster()); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
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
	if leader != h.s.remoteAddr(h.s.httpAddr) {
		if leader == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
			return
		}
		leader = fmt.Sprintf("%s://%s/lease?%s", h.scheme(), leader, q.Encode())
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
		h.httpError(e, w, http.StatusInternalServerError)
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

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w gzipResponseWriter) Flush() {
	w.Writer.(*gzip.Writer).Flush()
}

func (w gzipResponseWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

// determines if the client can accept compressed responses, and encodes accordingly
func gzipFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			inner.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		inner.ServeHTTP(gzw, r)
	})
}

// versionHeader takes a HTTP handler and returns a HTTP handler
// and adds the X-INFLUXBD-VERSION header to outgoing responses.
func versionHeader(inner http.Handler, h *handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-InfluxDB-Version", h.s.Version)
		inner.ServeHTTP(w, r)
	})
}

func requestID(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid := uuid.TimeUUID()
		r.Header.Set("Request-Id", uid.String())
		w.Header().Set("Request-Id", r.Header.Get("Request-Id"))

		inner.ServeHTTP(w, r)
	})
}

func logging(inner http.Handler, name string, weblog *zap.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)
		logLine := buildLogLine(l, r, start)
		weblog.Info(logLine)
	})
}

func recovery(inner http.Handler, name string, weblog *zap.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}

		defer func() {
			if err := recover(); err != nil {
				b := make([]byte, 1024)
				runtime.Stack(b, false)
				logLine := buildLogLine(l, r, start)
				weblog.Info(logLine, zap.Any("panic", err), zap.String("stack", string(b)))
			}
		}()

		inner.ServeHTTP(l, r)
	})
}

func (h *handler) scheme() string {
	if h.config.HTTPSEnabled {
		return "https"
	}
	return "http"
}

func (h *handler) dataScheme() string {
	if h.config.DataUseTLS {
		return "https"
	}
	return "http"
}

func (h *handler) httpError(err error, w http.ResponseWriter, status int) {
	if h.loggingEnabled {
		h.logger.Info("httpError", zap.Error(err))
	}

	errmsg := err.Error()
	if status/100 != 2 {
		sz := math.Min(float64(len(errmsg)), 1024.0)
		w.Header().Set("X-InfluxDB-Error", errmsg[:int(sz)])
	}

	errResp := &ErrorResponse{Err: errmsg}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(status)
	b, _ := json.Marshal(errResp)
	w.Write(b)
}

// ErrorResponse represents a error response.
type ErrorResponse struct {
	Err string `json:"error"`
}

func DecodeErrorResponse(body io.ReadCloser) error {
	errResp := &ErrorResponse{}
	if err := json.NewDecoder(body).Decode(errResp); err != nil {
		return err
	}
	return errors.New(errResp.Err)
}
