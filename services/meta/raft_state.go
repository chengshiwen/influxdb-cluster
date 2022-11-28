package meta

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/influxdata/influxdb/tcp"
	"go.uber.org/zap"
)

// Raft configuration.
const (
	raftLogCacheSize      = 512
	raftSnapshotsRetained = 2
	raftTransportMaxPool  = 3
	raftTransportTimeout  = 10 * time.Second
)

// raftState is a consensus strategy that uses a local raft implementation for
// consensus operations.
type raftState struct {
	wg        sync.WaitGroup
	config    *Config
	closing   chan struct{}
	raft      *raft.Raft
	transport *raft.NetworkTransport
	raftStore *raftboltdb.BoltStore
	raftLayer *raftLayer
	ln        net.Listener
	addr      string
	logger    *zap.Logger
	path      string
}

func newRaftState(c *Config, addr string) *raftState {
	return &raftState{
		config: c,
		addr:   addr,
	}
}

func (r *raftState) open(s *store, ln net.Listener) error {
	r.ln = ln
	r.closing = make(chan struct{})

	// Setup raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.raftAddr)
	config.LogOutput = io.Discard

	if r.config.ClusterTracing {
		config.LogOutput = os.Stdout
	}
	config.HeartbeatTimeout = time.Duration(r.config.HeartbeatTimeout)
	config.ElectionTimeout = time.Duration(r.config.ElectionTimeout)
	config.LeaderLeaseTimeout = time.Duration(r.config.LeaderLeaseTimeout)
	config.CommitTimeout = time.Duration(r.config.CommitTimeout)
	// Since we actually never call `removePeer` this is safe.
	// If in the future we decide to call remove peer we have to re-evaluate how to handle this
	config.ShutdownOnRemove = false

	// Build raft layer to multiplex listener.
	tlsC := r.config.TLSClientConfig()
	r.raftLayer = newRaftLayer(r.addr, tlsC, r.ln)

	// Create a transport layer
	r.transport = raft.NewNetworkTransport(r.raftLayer, raftTransportMaxPool, raftTransportTimeout, config.LogOutput)
	// Create the log store and stable store.
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(r.path, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	r.raftStore = stableStore

	// Wrap the store in a LogCache to improve performance.
	logStore, err := raft.NewLogCache(raftLogCacheSize, stableStore)
	if err != nil {
		return fmt.Errorf("new log cache: %s", err)
	}

	// Create the snapshot store.
	snapshots, err := raft.NewFileSnapshotStore(r.path, raftSnapshotsRetained, config.LogOutput)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Bootstrap now if we are in single server mode.
	if r.config.SingleServer {
		r.logger.Info("Starting in single server mode", zap.String("id", string(config.LocalID)), zap.String("addr", string(r.transport.LocalAddr())))
		configuration := r.bootstrapConfiguration()
		if err = raft.BootstrapCluster(config, logStore, stableStore, snapshots, r.transport, configuration); err != nil && err != raft.ErrCantBootstrap {
			return err
		}
	}

	// Create raft node.
	ra, err := raft.NewRaft(config, (*storeFSM)(s), logStore, stableStore, snapshots, r.transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	r.raft = ra

	r.wg.Add(1)
	go r.logLeaderChanges()

	return nil
}

func (r *raftState) bootstrapConfiguration() raft.Configuration {
	return raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(r.addr),
				Address: r.transport.LocalAddr(),
			},
		},
	}
}

func (r *raftState) bootstrap() error {
	if !r.config.SingleServer && r.leader() == "" {
		configuration := r.bootstrapConfiguration()
		future := r.raft.BootstrapCluster(configuration)
		return future.Error()
	}
	return nil
}

func (r *raftState) logLeaderChanges() {
	defer r.wg.Done()
	// Logs our current state (Node at 1.2.3.4:8088 [Follower])
	r.logger.Info("Raft state", zap.String("state", r.raft.String()))
	for {
		select {
		case <-r.closing:
			return
		case <-r.raft.LeaderCh():
			peers, err := r.peers()
			if err != nil {
				r.logger.Warn("Failed to lookup peers", zap.Error(err))
			}
			r.logger.Info("Raft state", zap.String("state", r.raft.String()), zap.Strings("peers", peers))
		}
	}
}

func (r *raftState) close() error {
	if r == nil {
		return nil
	}
	if r.closing != nil {
		close(r.closing)
	}
	r.wg.Wait()

	if r.transport != nil {
		r.transport.Close()
		r.transport = nil
	}

	// Shutdown raft.
	if r.raft != nil {
		if err := r.raft.Shutdown().Error(); err != nil {
			return err
		}
		r.raft = nil
	}

	if r.raftStore != nil {
		r.raftStore.Close()
		r.raftStore = nil
	}

	return nil
}

// apply applies a serialized command to the raft log.
func (r *raftState) apply(b []byte) error {
	// Apply to raft log.
	f := r.raft.Apply(b, 0)
	if err := f.Error(); err != nil {
		return err
	}

	// Return response if it's an error.
	// No other non-nil objects should be returned.
	resp := f.Response()
	if err, ok := resp.(error); ok {
		return err
	}
	if resp != nil {
		panic(fmt.Sprintf("unexpected response: %#v", resp))
	}

	return nil
}

func (r *raftState) lastIndex() uint64 {
	return r.raft.LastIndex()
}

func (r *raftState) snapshot() error {
	future := r.raft.Snapshot()
	return future.Error()
}

// addPeer adds addr to the list of peers in the cluster.
func (r *raftState) addPeer(addr string) error {
	future := r.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		r.logger.Error("Failed to get raft configuration", zap.Error(err))
		return err
	}

	for _, srv := range future.Configuration().Servers {
		if srv.Address == raft.ServerAddress(addr) {
			return nil
		}
	}

	fut := r.raft.AddVoter(raft.ServerID(addr), raft.ServerAddress(addr), 0, 0)
	if fut.Error() != nil {
		return fut.Error()
	}
	return nil
}

// removePeer removes addr from the list of peers in the cluster.
func (r *raftState) removePeer(addr string) error {
	future := r.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		r.logger.Error("Failed to get raft configuration", zap.Error(err))
		return err
	}

	for _, srv := range future.Configuration().Servers {
		if srv.Address == raft.ServerAddress(addr) {
			fut := r.raft.RemoveServer(srv.ID, 0, 0)
			if fut.Error() != nil {
				return fut.Error()
			}
			break
		}
	}
	return nil
}

func (r *raftState) peers() ([]string, error) {
	if r.raft == nil {
		return nil, nil
	}
	future := r.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var peers []string
	for _, srv := range future.Configuration().Servers {
		peers = append(peers, string(srv.Address))
	}
	return peers, nil
}

func (r *raftState) leader() string {
	if r.raft == nil {
		return ""
	}
	l, _ := r.raft.LeaderWithID()
	return string(l)
}

func (r *raftState) isLeader() bool {
	if r.raft == nil {
		return false
	}
	return r.raft.State() == raft.Leader
}

func (r *raftState) attemptLeadershipTransfer() bool {
	retryCount := 3
	for i := 0; i < retryCount; i++ {
		future := r.raft.LeadershipTransfer()
		if err := future.Error(); err != nil {
			r.logger.Error("Failed to transfer leadership attempt, will retry", zap.Int("attempt", i), zap.Int("retry_limit", retryCount), zap.Error(err))
		} else {
			r.logger.Info("Successfully transferred leadership", zap.Int("attempt", i), zap.Int("retry_limit", retryCount))
			return true
		}
	}
	return false
}

// raftLayer wraps the connection so it can be re-used for forwarding.
type raftLayer struct {
	addr   *raftLayerAddr
	tlsC   *tls.Config
	ln     net.Listener
	closed chan struct{}
}

type raftLayerAddr struct {
	addr string
}

func (r *raftLayerAddr) Network() string {
	return "tcp"
}

func (r *raftLayerAddr) String() string {
	return r.addr
}

// newRaftLayer returns a new instance of raftLayer.
func newRaftLayer(addr string, tlsC *tls.Config, ln net.Listener) *raftLayer {
	return &raftLayer{
		addr:   &raftLayerAddr{addr},
		tlsC:   tlsC,
		ln:     ln,
		closed: make(chan struct{}),
	}
}

// Addr returns the local address for the layer.
func (l *raftLayer) Addr() net.Addr {
	return l.addr
}

// Dial creates a new network connection.
func (l *raftLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := tcp.DialTLSTimeout("tcp", string(addr), l.tlsC, timeout)
	if err != nil {
		return nil, err
	}
	// Write a marker byte for raft messages.
	_, err = conn.Write([]byte{MuxHeader})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, err
}

// Accept waits for the next connection.
func (l *raftLayer) Accept() (net.Conn, error) { return l.ln.Accept() }

// Close closes the layer.
func (l *raftLayer) Close() error {
	if l.closed != nil {
		close(l.closed)
	}
	return nil
}
