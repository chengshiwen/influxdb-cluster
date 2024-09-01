package meta

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	internal "github.com/influxdata/influxdb/services/meta/internal"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
)

// Raft configuration.
const (
	raftRemoveGracePeriod = 5 * time.Second
)

type store struct {
	mu      sync.RWMutex
	closing chan struct{}

	config      *Config
	data        *Data
	raftState   *raftState
	dataChanged chan struct{}
	path        string
	opened      bool
	logger      *zap.Logger

	raftAddr string
	httpAddr string
}

// newStore will create a new metastore with the passed in config
func newStore(c *Config, httpAddr, raftAddr string) *store {
	s := store{
		data: &Data{
			Index: 1,
		},
		closing:     make(chan struct{}),
		dataChanged: make(chan struct{}),
		path:        c.Dir,
		config:      c,
		logger:      zap.NewNop(),
		httpAddr:    httpAddr,
		raftAddr:    raftAddr,
	}

	return &s
}

// WithLogger sets the logger for the store.
func (s *store) WithLogger(log *zap.Logger) {
	s.logger = log.With(zap.String("service", "metastore"))
}

// open opens and initializes the raft store.
func (s *store) open(raftln net.Listener) error {
	s.logger.Info("Using data dir", zap.String("path", s.path))

	if err := s.setOpen(); err != nil {
		return err
	}

	// Create the root directory if it doesn't already exist.
	if err := os.MkdirAll(s.path, 0777); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}

	// Open the raft store.
	if err := s.openRaft(raftln); err != nil {
		return fmt.Errorf("raft: %s", err)
	}

	// Wait for a leader to be elected so we know the raft log is loaded
	// and up to date
	if err := s.waitForLeader(0); err != nil {
		return err
	}

	// Make sure this server is in the list of metanodes
	if s.config.SingleServer && len(s.peers()) <= 1 {
		// we have to loop here because if the hostname has changed
		// raft will take a little bit to normalize so that this host
		// will be marked as the leader
		for {
			err := s.setMetaNode(s.httpAddr, s.raftAddr)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil
}

func (s *store) setOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check if store has already been opened.
	if s.opened {
		return ErrStoreOpen
	}
	s.opened = true
	return nil
}

// peers returns the raft peers known to this store
func (s *store) peers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil {
		return nil
	}
	if s.leader() == "" {
		return nil
	}
	peers, err := s.raftState.peers()
	if err != nil {
		return nil
	}
	return peers
}

func (s *store) openRaft(raftln net.Listener) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs := newRaftState(s.config, s.raftAddr)
	rs.logger = s.logger
	rs.path = s.path

	if err := rs.open(s, raftln); err != nil {
		return err
	}
	s.raftState = rs

	return nil
}

func (s *store) bootstrap() error {
	s.mu.RLock()
	if s.raftState == nil || s.raftState.raft == nil {
		s.mu.RUnlock()
		return fmt.Errorf("store not open")
	}
	s.mu.RUnlock()
	err := s.raftState.bootstrap()
	if err != nil {
		return err
	}
	return s.waitForLeader(0)
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.closing:
		// already closed
		return nil
	default:
		close(s.closing)
		return s.raftState.close()
	}
}

func (s *store) snapshot() (*Data, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Clone(), nil
}

// afterIndex returns a channel that will be closed to signal
// the caller when an updated snapshot is available.
func (s *store) afterIndex(index uint64) <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.data.Index {
		// Client needs update so return a closed channel.
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return s.dataChanged
}

// state returns the state of this raft peer.
func (s *store) state() raft.RaftState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil || s.raftState.raft == nil {
		return raft.Shutdown
	}
	return s.raftState.raft.State()
}

// waitForLeader sleeps until a leader is found or a timeout occurs.
// timeout == 0 means to wait forever.
func (s *store) waitForLeader(timeout time.Duration) error {
	// Begin timeout timer.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Continually check for leader until timeout.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return errors.New("closing")
		case <-timer.C:
			if timeout != 0 {
				return errors.New("timeout")
			}
		case <-ticker.C:
			if s.leader() != "" {
				return nil
			}
		}
	}
}

// isLeader returns true if the store is currently the leader.
func (s *store) isLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil || s.raftState.raft == nil {
		return false
	}
	return s.raftState.raft.State() == raft.Leader
}

// leader returns what the store thinks is the current leader. An empty
// string indicates no leader exists.
func (s *store) leader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil || s.raftState.raft == nil {
		return ""
	}
	l, _ := s.raftState.raft.LeaderWithID()
	return string(l)
}

// leaderHTTP returns the HTTP API connection info for the metanode
// that is the raft leader
func (s *store) leaderHTTP() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil {
		return ""
	}
	l, _ := s.raftState.raft.LeaderWithID()

	for _, n := range s.data.MetaNodes {
		if n.TCPAddr == string(l) {
			return n.Addr
		}
	}
	return ""
}

// metaServersHTTP will return the HTTP bind addresses of the
// meta servers in the cluster
func (s *store) metaServersHTTP() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var a []string
	for _, n := range s.data.MetaNodes {
		a = append(a, n.Addr)
	}
	return a
}

// otherMetaServersHTTP will return the HTTP bind addresses of the other
// meta servers in the cluster
func (s *store) otherMetaServersHTTP() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var a []string
	for _, n := range s.data.MetaNodes {
		if n.TCPAddr != s.raftAddr {
			a = append(a, n.Addr)
		}
	}
	return a
}

// dataServers will return the TCP bind addresses of the
// data servers in the cluster
func (s *store) dataServers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var a []string
	for _, n := range s.data.DataNodes {
		a = append(a, n.TCPAddr)
	}
	return a
}

// index returns the current store index.
func (s *store) index() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Index
}

// apply applies a command to raft.
func (s *store) apply(b []byte) error {
	if s.raftState == nil {
		return fmt.Errorf("store not open")
	}
	return s.raftState.apply(b)
}

// join adds a new server to the metaservice and raft
func (s *store) join(addr, raftAddr string) (*NodeInfo, error) {
	s.mu.RLock()
	for _, node := range s.data.MetaNodes {
		if node.Addr == addr && node.TCPAddr == raftAddr {
			s.mu.RUnlock()
			return &node, nil
		}
	}

	if s.raftState == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("store not open")
	}
	if err := s.raftState.addPeer(raftAddr); err != nil {
		s.mu.RUnlock()
		return nil, err
	}
	s.mu.RUnlock()

	if err := s.createMetaNode(addr, raftAddr); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, node := range s.data.MetaNodes {
		if node.Addr == addr && node.TCPAddr == raftAddr {
			return &node, nil
		}
	}
	return nil, ErrNodeNotFound
}

// leave removes a server from the metaservice and raft
func (s *store) leave(raftAddr string) error {
	s.mu.RLock()
	if s.raftState == nil {
		s.mu.RUnlock()
		return fmt.Errorf("store not open")
	}
	s.mu.RUnlock()

	// If we are the current leader, and we have any other peers (cluster has multiple
	// servers), we should do a RemovePeer to safely reduce the quorum size. If we are
	// not the leader, then we should issue our leave intention and wait to be removed
	// for some sane period of time.
	isLeader := s.isLeader()
	if isLeader && len(s.peers()) > 1 {
		s.mu.RLock()
		if s.raftState.attemptLeadershipTransfer() {
			isLeader = false
		} else {
			if err := s.raftState.removePeer(raftAddr); err != nil {
				s.logger.Error("Failed to remove peer", zap.String("addr", raftAddr), zap.Error(err))
			}
		}
		s.mu.RUnlock()
	}

	// If we were not leader, wait to be safely removed from the cluster.
	// We must wait to allow the raft replication to take place, otherwise
	// an immediate shutdown could cause a loss of quorum.
	if !isLeader {
		left := false
		limit := time.Now().Add(raftRemoveGracePeriod)
		for !left && time.Now().Before(limit) {
			// Sleep a while before we check.
			time.Sleep(50 * time.Millisecond)

			// Get the latest peers.
			s.mu.RLock()
			peers, err := s.raftState.peers()
			s.mu.RUnlock()
			if err != nil {
				s.logger.Error("Failed to get raft peers", zap.Error(err))
				break
			}

			// See if we are no longer included.
			left = true
			for _, p := range peers {
				if p == raftAddr {
					left = false
					break
				}
			}
		}
		if !left {
			s.logger.Warn("Failed to leave raft gracefully, timeout")
		}
	}

	return s.reset()
}

// remove a server from the metaservice and raft
func (s *store) remove(addr string) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	n, err := s.metaNodeByAddr(addr)
	if err != nil {
		return fmt.Errorf("node not found: %s", addr)
	}

	s.mu.RLock()
	if s.raftState == nil {
		s.mu.RUnlock()
		return fmt.Errorf("store not open")
	}
	s.mu.RUnlock()

	if err = s.deleteMetaNode(n.ID); err != nil {
		return err
	}

	s.mu.RLock()
	for _, node := range s.data.MetaNodes {
		if node.Addr == n.Addr || node.TCPAddr == n.TCPAddr {
			s.mu.RUnlock()
			return ErrNodeUnableToDropNode
		}
	}
	s.mu.RUnlock()

	if len(s.peers()) <= 1 {
		return s.reset()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if err = s.raftState.removePeer(n.TCPAddr); err != nil {
		return err
	}
	return nil
}

// reset the metaservice and raft
func (s *store) reset() error {
	s.mu.Lock()
	raftln := s.raftState.ln
	if err := s.raftState.close(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.data = &Data{
		Index: 1,
	}
	s.mu.Unlock()

	if err := os.RemoveAll(s.path); err != nil {
		return fmt.Errorf("remove all: %s", err)
	}
	if err := os.MkdirAll(s.path, 0777); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}
	if err := s.openRaft(raftln); err != nil {
		return fmt.Errorf("raft: %s", err)
	}

	return nil
}

// removeData removes a data server from the metaservice and raft
func (s *store) removeData(tcpAddr string) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	n, err := s.dataNodeByTCPAddr(tcpAddr)
	if err != nil {
		return fmt.Errorf("node not found: %s", tcpAddr)
	}

	if err = s.deleteDataNode(n.ID); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, node := range s.data.DataNodes {
		if node.Addr == n.Addr || node.TCPAddr == n.TCPAddr {
			return ErrNodeUnableToDropNode
		}
	}
	return nil
}

// updateData adds a new server to the metaservice and raft
func (s *store) updateData(addr, tcpAddr, oldTCPAddr string) (*NodeInfo, error) {
	if !s.isLeader() {
		return nil, raft.ErrNotLeader
	}

	n, err := s.dataNodeByTCPAddr(oldTCPAddr)
	if err != nil {
		return nil, fmt.Errorf("no data node with bind address %s exists", oldTCPAddr)
	}

	if err = s.updateDataNode(n.ID, addr, tcpAddr); err != nil {
		return nil, err
	}

	return s.dataNodeByTCPAddr(tcpAddr)
}

func (s *store) metaNodeByAddr(addr string) (*NodeInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, node := range s.data.MetaNodes {
		if node.Addr == addr {
			return &node, nil
		}
	}
	return nil, ErrNodeNotFound
}

func (s *store) dataNodeByTCPAddr(tcpAddr string) (*NodeInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, node := range s.data.DataNodes {
		if node.TCPAddr == tcpAddr {
			return &node, nil
		}
	}
	return nil, ErrNodeNotFound
}

func (s *store) dataNode(id uint64) (*NodeInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, node := range s.data.DataNodes {
		if node.ID == id {
			return &node, nil
		}
	}
	return nil, ErrNodeNotFound
}

// copyShard copies a shard in the metastore
func (s *store) copyShard(id, nodeID uint64) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}
	return s.copyShardOwner(id, nodeID)
}

// removeShard removes a shard in the metastore
func (s *store) removeShard(id, nodeID uint64) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}
	return s.removeShardOwner(id, nodeID)
}

// truncateShards truncates current shards
func (s *store) truncateShards(delay time.Duration) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}
	timestamp := time.Now().Add(delay)
	return s.truncateShardGroups(timestamp)
}

// createMetaNode is used by the join command to create the metanode in
// the metastore
func (s *store) createMetaNode(addr, raftAddr string) error {
	val := &internal.CreateMetaNodeCommand{
		HTTPAddr: proto.String(addr),
		TCPAddr:  proto.String(raftAddr),
		Rand:     proto.Uint64(uint64(rand.Int63())),
	}
	t := internal.Command_CreateMetaNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_CreateMetaNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// deleteMetaNode is used by the remove command to delete the metanode in
// the metastore
func (s *store) deleteMetaNode(id uint64) error {
	val := &internal.DeleteMetaNodeCommand{
		ID: proto.Uint64(id),
	}
	t := internal.Command_DeleteMetaNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_DeleteMetaNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// setMetaNode is used when the raft group has only a single peer. It will
// either create a metanode or update the information for the one metanode
// that is there. It's used because hostnames can change
func (s *store) setMetaNode(addr, raftAddr string) error {
	val := &internal.SetMetaNodeCommand{
		HTTPAddr: proto.String(addr),
		TCPAddr:  proto.String(raftAddr),
		Rand:     proto.Uint64(uint64(rand.Int63())),
	}
	t := internal.Command_SetMetaNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_SetMetaNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// deleteDataNode is used by the remove-data command to delete the datanode in
// the metastore
func (s *store) deleteDataNode(id uint64) error {
	val := &internal.DeleteDataNodeCommand{
		ID: proto.Uint64(id),
	}
	t := internal.Command_DeleteDataNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_DeleteDataNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// updateDataNode is used by the update-data command to update the datanode in
// the metastore
func (s *store) updateDataNode(id uint64, addr, tcpAddr string) error {
	val := &internal.UpdateDataNodeCommand{
		ID:       proto.Uint64(id),
		HTTPAddr: proto.String(addr),
		TCPAddr:  proto.String(tcpAddr),
	}
	t := internal.Command_UpdateDataNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_UpdateDataNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// copyShardOwner is used by the copy-shard command to copy a shard
func (s *store) copyShardOwner(id, nodeID uint64) error {
	val := &internal.CopyShardOwnerCommand{
		ID:     proto.Uint64(id),
		NodeID: proto.Uint64(nodeID),
	}
	t := internal.Command_CopyShardOwnerCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_CopyShardOwnerCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// removeShardOwner is used by the remove-shard command to remove a shard
func (s *store) removeShardOwner(id, nodeID uint64) error {
	val := &internal.RemoveShardOwnerCommand{
		ID:     proto.Uint64(id),
		NodeID: proto.Uint64(nodeID),
	}
	t := internal.Command_RemoveShardOwnerCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_RemoveShardOwnerCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// truncateShardGroups is used by the truncate-shards command to truncate shard groups
func (s *store) truncateShardGroups(timestamp time.Time) error {
	val := &internal.TruncateShardGroupsCommand{
		Timestamp: proto.Int64(timestamp.UnixNano()),
	}
	t := internal.Command_TruncateShardGroupsCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_TruncateShardGroupsCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// createUser is used to create a user.
func (s *store) createUser(name, password string, admin bool) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return err
	}

	val := &internal.CreateUserCommand{
		Name:  proto.String(name),
		Hash:  proto.String(string(hash)),
		Admin: proto.Bool(admin),
	}
	t := internal.Command_CreateUserCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_CreateUserCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// dropUser is used to drop a user.
func (s *store) dropUser(name string) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	val := &internal.DropUserCommand{
		Name: proto.String(name),
	}
	t := internal.Command_DropUserCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_DropUserCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// updateUser is used to update a user.
func (s *store) updateUser(name, password string) error {
	if !s.isLeader() {
		return raft.ErrNotLeader
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return err
	}

	val := &internal.UpdateUserCommand{
		Name: proto.String(name),
		Hash: proto.String(string(hash)),
	}
	t := internal.Command_UpdateUserCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_UpdateUserCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

func (s *store) adminUserExists() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.AdminUserExists()
}

func (s *store) authenticate(username, password string) (User, error) {
	// Find user.
	s.mu.RLock()
	userInfo := s.data.user(username)
	s.mu.RUnlock()
	if userInfo == nil {
		return nil, ErrUserNotFound
	}

	// Compare password with user hash.
	if err := bcrypt.CompareHashAndPassword([]byte(userInfo.Hash), []byte(password)); err != nil {
		return nil, ErrAuthenticate
	}
	return userInfo, nil
}

func (s *store) user(name string) (User, error) {
	s.mu.RLock()
	u := s.data.user(name)
	s.mu.RUnlock()
	if u == nil {
		return nil, ErrUserNotFound
	}
	return u, nil
}

func (s *store) users() []UserInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Users
}

func (s *store) nodeID() uint64 {
	n, err := s.metaNodeByAddr(s.httpAddr)
	if err != nil {
		return 0
	}
	return n.ID
}

func (s *store) clusterID() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.ClusterID
}

func (s *store) status() *MetaNodeStatus {
	return &MetaNodeStatus{
		NodeType: NodeTypeMeta,
		Leader:   s.leader(),
		HTTPAddr: s.httpAddr,
		RaftAddr: s.raftAddr,
		Peers:    s.peers(),
	}
}

func (s *store) cluster() *ClusterInfo {
	s.mu.RLock()
	dns, mns := s.data.DataNodes, s.data.MetaNodes
	s.mu.RUnlock()
	ci := &ClusterInfo{}
	if s.leader() != "" && len(dns) > 0 {
		data := make([]*DataNodeInfo, len(dns))
		for i, n := range dns {
			data[i] = &DataNodeInfo{
				ID:       n.ID,
				TCPAddr:  n.TCPAddr,
				HTTPAddr: n.Addr,
				Status:   NodeStatusJoined,
			}
		}
		ci.Data = data
	}
	if s.leader() != "" && len(mns) > 0 {
		meta := make([]*MetaNodeInfo, len(mns))
		for i, n := range mns {
			meta[i] = &MetaNodeInfo{
				ID:      n.ID,
				Addr:    n.Addr,
				TCPAddr: n.TCPAddr,
			}
		}
		ci.Meta = meta
	}
	return ci
}

func (s *store) shards() []*ClusterShardInfo {
	s.mu.RLock()
	dis := s.data.Databases
	s.mu.RUnlock()
	var shardInfos []*ClusterShardInfo
	for _, di := range dis {
		for _, rpi := range di.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				if sgi.Deleted() {
					continue
				}
				if rpi.Duration != 0 && sgi.EndTime.Add(rpi.Duration).Before(time.Now().UTC()) {
					continue
				}
				for _, si := range sgi.Shards {
					shardInfo := s.shardInfo(di, rpi, sgi, si)
					shardInfos = append(shardInfos, shardInfo)
				}
			}
		}
	}
	return shardInfos
}

func (s *store) shard(id uint64) *ClusterShardInfo {
	s.mu.RLock()
	dis := s.data.Databases
	s.mu.RUnlock()
	for _, di := range dis {
		for _, rpi := range di.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, si := range sgi.Shards {
					if si.ID == id {
						return s.shardInfo(di, rpi, sgi, si)
					}
				}
			}
		}
	}
	return nil
}

func (s *store) shardInfo(di DatabaseInfo, rpi RetentionPolicyInfo, sgi ShardGroupInfo, si ShardInfo) *ClusterShardInfo {
	var expire time.Time
	if rpi.Duration != 0 {
		expire = sgi.EndTime.Add(rpi.Duration)
	}
	owners := make([]*ShardOwnerInfo, len(si.Owners))
	for i, owner := range si.Owners {
		n, _ := s.dataNode(owner.NodeID)
		owners[i] = &ShardOwnerInfo{
			ID:      owner.NodeID,
			TCPAddr: n.TCPAddr,
		}
	}
	return &ClusterShardInfo{
		ID:              si.ID,
		Database:        di.Name,
		RetentionPolicy: rpi.Name,
		ReplicaN:        rpi.ReplicaN,
		ShardGroupID:    sgi.ID,
		StartTime:       sgi.StartTime,
		EndTime:         sgi.EndTime,
		ExpireTime:      expire,
		TruncatedAt:     sgi.TruncatedAt,
		Owners:          owners,
	}
}
