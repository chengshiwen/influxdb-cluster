// Package meta provides control over meta data for InfluxDB,
// such as controlling databases, retention policies, users, etc.
package meta

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/httputil"
	internal "github.com/influxdata/influxdb/services/meta/internal"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

const (
	// errSleep is the time to sleep after we've failed on every metaserver
	// before making another pass
	errSleep = time.Second

	// maxRetries is the maximum number of attemps to make before returning
	// a failure to the caller
	maxRetries = 10

	// SaltBytes is the number of bytes used for salts.
	SaltBytes = 32

	clientFile = "client.json"

	// ShardGroupDeletedExpiration is the amount of time before a shard group info will be removed from cached
	// data after it has been marked deleted (2 weeks).
	ShardGroupDeletedExpiration = -2 * 7 * 24 * time.Hour
)

var (
	// ErrServiceUnavailable is returned when the meta service is unavailable.
	ErrServiceUnavailable = errors.New("meta service unavailable")

	// ErrService is returned when the meta service returns an error.
	ErrService = errors.New("meta service error")
)

// Client is used to execute commands on and read data from
// a meta service cluster.
type Client struct {
	logger *zap.Logger

	mu        sync.RWMutex
	closing   chan struct{}
	changed   chan struct{}
	cacheData *Data

	// Authentication cache.
	authCache map[string]authUser

	nodeID      uint64
	metaServers []string
	opened      bool

	config  *Config
	client  *httputil.Client
	tcpAddr string

	path string
}

type authUser struct {
	bhash string
	salt  []byte
	hash  []byte
}

// NewClient returns a new *Client.
func NewClient(config *Config) *Client {
	return &Client{
		cacheData: &Data{},
		closing:   make(chan struct{}),
		changed:   make(chan struct{}),
		logger:    zap.NewNop(),
		authCache: make(map[string]authUser),
		config:    config,
		client: httputil.NewClient(httputil.Config{
			AuthEnabled: config.MetaAuthEnabled,
			AuthType:    httputil.AuthTypeJWT,
			Secret:      config.MetaInternalSharedSecret,
			UserAgent:   "InfluxDB Meta Client",
			UseTLS:      config.MetaTLSEnabled,
			SkipTLS:     config.MetaInsecureTLS,
		}),
		path: config.Dir,
	}
}

// Open a connection to a meta service cluster.
func (c *Client) Open() error {
	if c.opened {
		return errors.New("meta client already open")
	}

	c.logger.Info("Connecting to meta service")

	// Try to load from disk
	if err := c.Load(); err != nil {
		c.logger.Warn("Could not connect to meta service", zap.Error(err))
		if !os.IsNotExist(err) {
			return err
		}
	}

	if len(c.metaServers) > 0 {
		c.cacheData = c.retryUntilSnapshot(0)
		c.updateNodeID()
		c.opened = true
	}

	go c.pollForUpdates()
	return nil
}

// Close the meta service cluster connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.client.CloseIdleConnections()

	select {
	case <-c.closing:
		return nil
	default:
		close(c.closing)
	}

	return nil
}

// NodeID returns the client's node ID.
func (c *Client) NodeID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodeID
}

// SetMetaServers updates the meta servers on the client.
func (c *Client) SetMetaServers(a []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metaServers = a
	if len(c.metaServers) == 0 {
		c.opened = false
	}
}

// MetaServers returns the meta servers on the client.
func (c *Client) MetaServers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.metaServers
}

// TCPAddr returns the client's tcp addr.
func (c *Client) TCPAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tcpAddr
}

// SetTCPAddr updates the tcp addr on the client.
func (c *Client) SetTCPAddr(tcpAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tcpAddr = tcpAddr
}

// Ping will hit the ping endpoint for the metaservice and return nil if
// it returns 200. If checkAllMetaServers is set to true, it will hit the
// ping endpoint and tell it to verify the health of all metaservers in the
// cluster
func (c *Client) Ping(checkAllMetaServers bool) error {
	c.mu.RLock()
	if len(c.metaServers) == 0 {
		c.mu.RUnlock()
		return ErrServiceUnavailable
	}
	server := c.metaServers[0]
	c.mu.RUnlock()
	url := c.url(server) + "/ping"
	if checkAllMetaServers {
		url = url + "?all=true"
	}

	resp, err := c.client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return fmt.Errorf(string(b))
}

// AcquireLease attempts to acquire the specified lease.
// A lease is a logical concept that can be used by anything that needs to limit
// execution to a single node.  E.g., the CQ service on all nodes may ask for
// the "ContinuousQuery" lease. Only the node that acquires it will run CQs.
// NOTE: Leases are not managed through the CP system and are not fully
// consistent.  Any actions taken after acquiring a lease must be idempotent.
func (c *Client) AcquireLease(name string) (l *Lease, err error) {
	for n := 1; n < 11; n++ {
		if l, err = c.acquireLease(name); err == ErrServiceUnavailable || err == ErrService {
			// exponential backoff
			d := time.Duration(math.Pow(10, float64(n))) * time.Millisecond
			time.Sleep(d)
			continue
		}
		break
	}
	return
}

func (c *Client) acquireLease(name string) (*Lease, error) {
	c.mu.RLock()
	if len(c.metaServers) == 0 {
		c.mu.RUnlock()
		return nil, ErrServiceUnavailable
	}
	server := c.metaServers[0]
	url := fmt.Sprintf("%s/lease?name=%s&nodeid=%d", c.url(server), name, c.nodeID)
	c.mu.RUnlock()

	resp, err := c.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusConflict:
		err = errors.New("another node owns the lease")
	case http.StatusServiceUnavailable:
		return nil, ErrServiceUnavailable
	case http.StatusBadRequest:
		b, e := io.ReadAll(resp.Body)
		if e != nil {
			return nil, e
		}
		return nil, fmt.Errorf("meta service: %s", string(b))
	case http.StatusInternalServerError:
		return nil, errors.New("meta service internal error")
	default:
		return nil, errors.New("unrecognized meta service error")
	}

	// Read lease JSON from response body.
	b, e := io.ReadAll(resp.Body)
	if e != nil {
		return nil, e
	}
	// Unmarshal JSON into a Lease.
	l := &Lease{}
	if e = json.Unmarshal(b, l); e != nil {
		return nil, e
	}

	return l, err
}

func (c *Client) data() *Data {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData
}

// ClusterID returns the ID of the cluster it's connected to.
func (c *Client) ClusterID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cacheData.ClusterID
}

// DataNode returns a node by id.
func (c *Client) DataNode(id uint64) (*NodeInfo, error) {
	for _, n := range c.data().DataNodes {
		if n.ID == id {
			return &n, nil
		}
	}
	return nil, ErrNodeNotFound
}

// DataNodes returns the data nodes' info.
func (c *Client) DataNodes() []NodeInfo {
	return c.data().DataNodes
}

// CreateDataNode will create a new data node in the metastore
func (c *Client) CreateDataNode(httpAddr, tcpAddr string) (*NodeInfo, error) {
	cmd := &internal.CreateDataNodeCommand{
		HTTPAddr: proto.String(httpAddr),
		TCPAddr:  proto.String(tcpAddr),
	}

	if err := c.retryUntilExec(internal.Command_CreateDataNodeCommand, internal.E_CreateDataNodeCommand_Command, cmd); err != nil {
		return nil, err
	}

	n, err := c.DataNodeByTCPAddr(tcpAddr)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.tcpAddr == tcpAddr {
		c.nodeID = n.ID
	}
	c.mu.Unlock()

	return n, nil
}

// DataNodeByHTTPAddr returns the data node with the give http bind address
func (c *Client) DataNodeByHTTPAddr(httpAddr string) (*NodeInfo, error) {
	for _, n := range c.DataNodes() {
		if n.Addr == httpAddr {
			return &n, nil
		}
	}

	return nil, ErrNodeNotFound
}

// DataNodeByTCPAddr returns the data node with the give tcp bind address
func (c *Client) DataNodeByTCPAddr(tcpAddr string) (*NodeInfo, error) {
	for _, n := range c.DataNodes() {
		if n.TCPAddr == tcpAddr {
			return &n, nil
		}
	}

	return nil, ErrNodeNotFound
}

// DeleteDataNode deletes a data node from the cluster.
func (c *Client) DeleteDataNode(id uint64) error {
	cmd := &internal.DeleteDataNodeCommand{
		ID: proto.Uint64(id),
	}

	return c.retryUntilExec(internal.Command_DeleteDataNodeCommand, internal.E_DeleteDataNodeCommand_Command, cmd)
}

// MetaNodes returns the meta nodes' info.
func (c *Client) MetaNodes() []NodeInfo {
	return c.data().MetaNodes
}

// CreateMetaNode will create a new meta node in the metastore
func (c *Client) CreateMetaNode(httpAddr, tcpAddr string) (*NodeInfo, error) {
	cmd := &internal.CreateMetaNodeCommand{
		HTTPAddr: proto.String(httpAddr),
		TCPAddr:  proto.String(tcpAddr),
		Rand:     proto.Uint64(uint64(rand.Int63())),
	}

	if err := c.retryUntilExec(internal.Command_CreateMetaNodeCommand, internal.E_CreateMetaNodeCommand_Command, cmd); err != nil {
		return nil, err
	}

	n := c.MetaNodeByAddr(httpAddr)
	if n == nil {
		return nil, errors.New("new meta node not found")
	}

	return n, nil
}

// MetaNodeByAddr returns the meta node's info.
func (c *Client) MetaNodeByAddr(addr string) *NodeInfo {
	for _, n := range c.data().MetaNodes {
		if n.Addr == addr {
			return &n
		}
	}
	return nil
}

// DeleteMetaNode deletes a meta node from the cluster.
func (c *Client) DeleteMetaNode(id uint64) error {
	cmd := &internal.DeleteMetaNodeCommand{
		ID: proto.Uint64(id),
	}

	return c.retryUntilExec(internal.Command_DeleteMetaNodeCommand, internal.E_DeleteMetaNodeCommand_Command, cmd)
}

// Database returns info for the requested database.
func (c *Client) Database(name string) *DatabaseInfo {
	for _, d := range c.data().Databases {
		if d.Name == name {
			return &d
		}
	}

	return nil
}

// Databases returns a list of all database infos.
func (c *Client) Databases() []DatabaseInfo {
	dbs := c.data().Databases
	if dbs == nil {
		return []DatabaseInfo{}
	}
	return dbs
}

// CreateDatabase creates a database or returns it if it already exists
func (c *Client) CreateDatabase(name string) (*DatabaseInfo, error) {
	if db := c.Database(name); db != nil {
		return db, nil
	}

	cmd := &internal.CreateDatabaseCommand{
		Name: proto.String(name),
	}

	err := c.retryUntilExec(internal.Command_CreateDatabaseCommand, internal.E_CreateDatabaseCommand_Command, cmd)
	if err != nil {
		return nil, err
	}

	db := c.Database(name)

	return db, nil
}

// CreateDatabaseWithRetentionPolicy creates a database with the specified
// retention policy.
//
// When creating a database with a retention policy, the retention policy will
// always be set to default. Therefore if the caller provides a retention policy
// that already exists on the database, but that retention policy is not the
// default one, an error will be returned.
//
// This call is only idempotent when the caller provides the exact same
// retention policy, and that retention policy is already the default for the
// database.
func (c *Client) CreateDatabaseWithRetentionPolicy(name string, spec *RetentionPolicySpec) (*DatabaseInfo, error) {
	if spec == nil {
		return nil, errors.New("CreateDatabaseWithRetentionPolicy called with nil spec")
	}
	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	// No existing retention policies, so we can create the provided policy as
	// the new default policy.
	rpi := spec.NewRetentionPolicyInfo()

	db := c.Database(name)
	if db != nil {
		if len(db.RetentionPolicies) > 0 && !spec.Matches(db.RetentionPolicy(rpi.Name)) {
			// In this case we already have a retention policy on the database and
			// the provided retention policy does not match it. Therefore, this call
			// is not idempotent and we need to return an error.
			return nil, ErrRetentionPolicyConflict
		}

		// If a non-default retention policy was passed in that already exists then
		// it's an error regardless of if the exact same retention policy is
		// provided. CREATE DATABASE WITH RETENTION POLICY should only be used to
		// create DEFAULT retention policies.
		if db.DefaultRetentionPolicy != rpi.Name {
			return nil, ErrRetentionPolicyConflict
		}
	}

	cmd := &internal.CreateDatabaseCommand{
		Name:            proto.String(name),
		RetentionPolicy: rpi.marshal(),
	}

	err := c.retryUntilExec(internal.Command_CreateDatabaseCommand, internal.E_CreateDatabaseCommand_Command, cmd)
	if err != nil {
		return nil, err
	}

	// Refresh the database info.
	db = c.Database(name)

	return db, nil
}

// DropDatabase deletes a database.
func (c *Client) DropDatabase(name string) error {
	cmd := &internal.DropDatabaseCommand{
		Name: proto.String(name),
	}

	return c.retryUntilExec(internal.Command_DropDatabaseCommand, internal.E_DropDatabaseCommand_Command, cmd)
}

// CreateRetentionPolicy creates a retention policy on the specified database.
func (c *Client) CreateRetentionPolicy(database string, spec *RetentionPolicySpec, makeDefault bool) (*RetentionPolicyInfo, error) {
	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	rp := spec.NewRetentionPolicyInfo()
	cmd := &internal.CreateRetentionPolicyCommand{
		Database:        proto.String(database),
		RetentionPolicy: rp.marshal(),
		Default:         proto.Bool(makeDefault),
	}

	if err := c.retryUntilExec(internal.Command_CreateRetentionPolicyCommand, internal.E_CreateRetentionPolicyCommand_Command, cmd); err != nil {
		return nil, err
	}

	return c.RetentionPolicy(database, spec.Name)
}

// RetentionPolicy returns the requested retention policy info.
func (c *Client) RetentionPolicy(database, name string) (rpi *RetentionPolicyInfo, err error) {
	db := c.Database(database)
	if db == nil {
		return nil, influxdb.ErrDatabaseNotFound(database)
	}

	return db.RetentionPolicy(name), nil
}

// DropRetentionPolicy drops a retention policy from a database.
func (c *Client) DropRetentionPolicy(database, name string) error {
	cmd := &internal.DropRetentionPolicyCommand{
		Database: proto.String(database),
		Name:     proto.String(name),
	}

	return c.retryUntilExec(internal.Command_DropRetentionPolicyCommand, internal.E_DropRetentionPolicyCommand_Command, cmd)
}

// UpdateRetentionPolicy updates a retention policy.
func (c *Client) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate, makeDefault bool) error {
	var newName *string
	if rpu.Name != nil {
		newName = rpu.Name
	}

	var duration *int64
	if rpu.Duration != nil {
		value := int64(*rpu.Duration)
		duration = &value
	}

	var replicaN *uint32
	if rpu.ReplicaN != nil {
		value := uint32(*rpu.ReplicaN)
		replicaN = &value
	}

	var shardDuration *int64
	if rpu.ShardGroupDuration != nil {
		value := int64(*rpu.ShardGroupDuration)
		shardDuration = &value
	}

	cmd := &internal.UpdateRetentionPolicyCommand{
		Database:           proto.String(database),
		Name:               proto.String(name),
		NewName:            newName,
		Duration:           duration,
		ReplicaN:           replicaN,
		ShardGroupDuration: shardDuration,
		Default:            proto.Bool(makeDefault),
	}

	return c.retryUntilExec(internal.Command_UpdateRetentionPolicyCommand, internal.E_UpdateRetentionPolicyCommand_Command, cmd)
}

// Users returns a slice of UserInfo representing the currently known users.
func (c *Client) Users() []UserInfo {
	users := c.data().Users

	if users == nil {
		return []UserInfo{}
	}
	return users
}

// user returns the user info with the given name, or ErrUserNotFound.
func (c *Client) user(name string) (*UserInfo, error) {
	for _, u := range c.data().Users {
		if u.Name == name {
			return &u, nil
		}
	}

	return nil, ErrUserNotFound
}

// User returns the user with the given name, or ErrUserNotFound.
func (c *Client) User(name string) (User, error) {
	u, err := c.user(name)
	if err != nil {
		return nil, err
	}
	return u, nil
}

// bcryptCost is the cost associated with generating password with bcrypt.
// This setting is lowered during testing to improve test suite performance.
var bcryptCost = bcrypt.DefaultCost

// hashWithSalt returns a salted hash of password using salt.
func (c *Client) hashWithSalt(salt []byte, password string) []byte {
	hasher := sha256.New()
	hasher.Write(salt)
	hasher.Write([]byte(password))
	return hasher.Sum(nil)
}

// saltedHash returns a salt and salted hash of password.
func (c *Client) saltedHash(password string) (salt, hash []byte, err error) {
	salt = make([]byte, SaltBytes)
	if _, err := io.ReadFull(crand.Reader, salt); err != nil {
		return nil, nil, err
	}

	return salt, c.hashWithSalt(salt, password), nil
}

// CreateUser adds a user with the given name and password and admin status.
func (c *Client) CreateUser(name, password string, admin bool) (User, error) {
	// See if the user already exists.
	if u := c.data().user(name); u != nil {
		if err := bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte(password)); err != nil || u.Admin != admin {
			return nil, ErrUserExists
		}
		return u, nil
	}

	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return nil, err
	}

	if err := c.retryUntilExec(internal.Command_CreateUserCommand, internal.E_CreateUserCommand_Command,
		&internal.CreateUserCommand{
			Name:  proto.String(name),
			Hash:  proto.String(string(hash)),
			Admin: proto.Bool(admin),
		},
	); err != nil {
		return nil, err
	}

	return c.user(name)
}

// UpdateUser updates the password of an existing user.
func (c *Client) UpdateUser(name, password string) error {
	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return err
	}

	return c.retryUntilExec(internal.Command_UpdateUserCommand, internal.E_UpdateUserCommand_Command,
		&internal.UpdateUserCommand{
			Name: proto.String(name),
			Hash: proto.String(string(hash)),
		},
	)
}

// DropUser removes the user with the given name.
func (c *Client) DropUser(name string) error {
	return c.retryUntilExec(internal.Command_DropUserCommand, internal.E_DropUserCommand_Command,
		&internal.DropUserCommand{
			Name: proto.String(name),
		},
	)
}

// SetPrivilege sets a privilege for the given user on the given database.
func (c *Client) SetPrivilege(username, database string, p influxql.Privilege) error {
	return c.retryUntilExec(internal.Command_SetPrivilegeCommand, internal.E_SetPrivilegeCommand_Command,
		&internal.SetPrivilegeCommand{
			Username:  proto.String(username),
			Database:  proto.String(database),
			Privilege: proto.Int32(int32(p)),
		},
	)
}

// SetAdminPrivilege sets or unsets admin privilege to the given username.
func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	return c.retryUntilExec(internal.Command_SetAdminPrivilegeCommand, internal.E_SetAdminPrivilegeCommand_Command,
		&internal.SetAdminPrivilegeCommand{
			Username: proto.String(username),
			Admin:    proto.Bool(admin),
		},
	)
}

// UserPrivileges returns the privileges for a user mapped by database name.
func (c *Client) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	p, err := c.data().UserPrivileges(username)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// UserPrivilege returns the privilege for the given user on the given database.
func (c *Client) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	p, err := c.data().UserPrivilege(username, database)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// AdminUserExists returns true if any user has admin privilege.
func (c *Client) AdminUserExists() bool {
	return c.data().AdminUserExists()
}

// Authenticate returns a UserInfo if the username and password match an existing entry.
func (c *Client) Authenticate(username, password string) (User, error) {
	// Find user.
	userInfo := c.data().user(username)
	if userInfo == nil {
		return nil, ErrUserNotFound
	}

	// Check the local auth cache first.
	c.mu.RLock()
	au, ok := c.authCache[username]
	c.mu.RUnlock()
	if ok {
		// verify the password using the cached salt and hash
		if bytes.Equal(c.hashWithSalt(au.salt, password), au.hash) {
			return userInfo, nil
		}

		// fall through to requiring a full bcrypt hash for invalid passwords
	}

	// Compare password with user hash.
	if err := bcrypt.CompareHashAndPassword([]byte(userInfo.Hash), []byte(password)); err != nil {
		return nil, ErrAuthenticate
	}

	// generate a salt and hash of the password for the cache
	salt, hashed, err := c.saltedHash(password)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.authCache[username] = authUser{salt: salt, hash: hashed, bhash: userInfo.Hash}
	c.mu.Unlock()
	return userInfo, nil
}

// UserCount returns the number of users stored.
func (c *Client) UserCount() int {
	return len(c.data().Users)
}

// ShardIDs returns a list of all shard ids.
func (c *Client) ShardIDs() []uint64 {
	var a []uint64
	for _, dbi := range c.data().Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, si := range sgi.Shards {
					a = append(a, si.ID)
				}
			}
		}
	}
	sort.Sort(uint64Slice(a))
	return a
}

// ShardGroupsByTimeRange returns a list of all shard groups on a database and policy that may contain data
// for the specified time range. Shard groups are sorted by start time.
func (c *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []ShardGroupInfo, err error) {
	// Find retention policy.
	rpi, err := c.data().RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, influxdb.ErrRetentionPolicyNotFound(policy)
	}
	groups := make([]ShardGroupInfo, 0, len(rpi.ShardGroups))
	for _, g := range rpi.ShardGroups {
		if g.Deleted() || !g.Overlaps(min, max) {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// ShardsByTimeRange returns a slice of shards that may contain data in the time range.
func (c *Client) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []ShardInfo, err error) {
	m := make(map[*ShardInfo]struct{})
	for _, mm := range sources.Measurements() {
		groups, err := c.ShardGroupsByTimeRange(mm.Database, mm.RetentionPolicy, tmin, tmax)
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			for i := range g.Shards {
				m[&g.Shards[i]] = struct{}{}
			}
		}
	}

	a = make([]ShardInfo, 0, len(m))
	for sh := range m {
		a = append(a, *sh)
	}

	return a, nil
}

// DropShard deletes a shard by ID.
func (c *Client) DropShard(id uint64) error {
	cmd := &internal.DropShardCommand{
		ID: proto.Uint64(id),
	}

	return c.retryUntilExec(internal.Command_DropShardCommand, internal.E_DropShardCommand_Command, cmd)
}

// TruncateShardGroups truncates any shard group that could contain timestamps beyond t.
func (c *Client) TruncateShardGroups(t time.Time) error {
	return c.retryUntilExec(internal.Command_TruncateShardGroupsCommand, internal.E_TruncateShardGroupsCommand_Command,
		&internal.TruncateShardGroupsCommand{
			Timestamp: proto.Int64(t.UnixNano()),
		},
	)
}

// PruneShardGroups remove deleted shard groups from the data store.
func (c *Client) PruneShardGroups() error {
	return c.retryUntilExec(internal.Command_PruneShardGroupsCommand, internal.E_PruneShardGroupsCommand_Command,
		&internal.PruneShardGroupsCommand{},
	)
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (c *Client) CreateShardGroup(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	if sg, _ := c.data().ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		return sg, nil
	}

	cmd := &internal.CreateShardGroupCommand{
		Database:  proto.String(database),
		Policy:    proto.String(policy),
		Timestamp: proto.Int64(timestamp.UnixNano()),
	}

	if err := c.retryUntilExec(internal.Command_CreateShardGroupCommand, internal.E_CreateShardGroupCommand_Command, cmd); err != nil {
		return nil, err
	}

	rpi, err := c.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, errors.New("retention policy deleted after shard group created")
	}

	sgi := rpi.ShardGroupByTimestamp(timestamp)
	return sgi, nil
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	cmd := &internal.DeleteShardGroupCommand{
		Database:     proto.String(database),
		Policy:       proto.String(policy),
		ShardGroupID: proto.Uint64(id),
	}

	return c.retryUntilExec(internal.Command_DeleteShardGroupCommand, internal.E_DeleteShardGroupCommand_Command, cmd)
}

// PrecreateShardGroups creates shard groups whose endtime is before the 'to' time passed in, but
// is yet to expire before 'from'. This is to avoid the need for these shards to be created when data
// for the corresponding time range arrives. Shard creation involves Raft consensus, and precreation
// avoids taking the hit at write-time.
func (c *Client) PrecreateShardGroups(from, to time.Time) error {
	for _, di := range c.data().Databases {
		for _, rp := range di.RetentionPolicies {
			if len(rp.ShardGroups) == 0 {
				// No data was ever written to this group, or all groups have been deleted.
				continue
			}
			g := rp.ShardGroups[len(rp.ShardGroups)-1] // Get the last group in time.
			if !g.Deleted() && g.EndTime.Before(to) && g.EndTime.After(from) {
				// Group is not deleted, will end before the future time, but is still yet to expire.
				// This last check is important, so the system doesn't create shards groups wholly
				// in the past.

				// Create successive shard group.
				nextShardGroupTime := g.EndTime.Add(1 * time.Nanosecond)
				// if it already exists, continue
				if sg, _ := c.data().ShardGroupByTimestamp(di.Name, rp.Name, nextShardGroupTime); sg != nil {
					c.logger.Info("Shard group already exists",
						logger.ShardGroup(sg.ID),
						logger.Database(di.Name),
						logger.RetentionPolicy(rp.Name))
					continue
				}
				newGroup, err := c.CreateShardGroup(di.Name, rp.Name, nextShardGroupTime)
				if err != nil || newGroup == nil {
					c.logger.Info("Failed to precreate successive shard group",
						zap.Uint64("group_id", g.ID), zap.Error(err))
					continue
				}
				c.logger.Info("New shard group successfully precreated",
					logger.ShardGroup(newGroup.ID),
					logger.Database(di.Name),
					logger.RetentionPolicy(rp.Name))
			}
		}
	}

	return nil
}

// ShardOwner returns the owning shard group info for a specific shard.
func (c *Client) ShardOwner(shardID uint64) (database, policy string, sgi *ShardGroupInfo) {
	for _, dbi := range c.data().Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, g := range rpi.ShardGroups {
				if g.Deleted() {
					continue
				}

				for _, sh := range g.Shards {
					if sh.ID == shardID {
						database = dbi.Name
						policy = rpi.Name
						sgi = &g
						return
					}
				}
			}
		}
	}
	return
}

// CreateContinuousQuery saves a continuous query with the given name for the given database.
func (c *Client) CreateContinuousQuery(database, name, query string) error {
	return c.retryUntilExec(internal.Command_CreateContinuousQueryCommand, internal.E_CreateContinuousQueryCommand_Command,
		&internal.CreateContinuousQueryCommand{
			Database: proto.String(database),
			Name:     proto.String(name),
			Query:    proto.String(query),
		},
	)
}

// DropContinuousQuery removes the continuous query with the given name on the given database.
func (c *Client) DropContinuousQuery(database, name string) error {
	return c.retryUntilExec(internal.Command_DropContinuousQueryCommand, internal.E_DropContinuousQueryCommand_Command,
		&internal.DropContinuousQueryCommand{
			Database: proto.String(database),
			Name:     proto.String(name),
		},
	)
}

// CreateSubscription creates a subscription against the given database and retention policy.
func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return c.retryUntilExec(internal.Command_CreateSubscriptionCommand, internal.E_CreateSubscriptionCommand_Command,
		&internal.CreateSubscriptionCommand{
			Database:        proto.String(database),
			RetentionPolicy: proto.String(rp),
			Name:            proto.String(name),
			Mode:            proto.String(mode),
			Destinations:    destinations,
		},
	)
}

// DropSubscription removes the named subscription from the given database and retention policy.
func (c *Client) DropSubscription(database, rp, name string) error {
	return c.retryUntilExec(internal.Command_DropSubscriptionCommand, internal.E_DropSubscriptionCommand_Command,
		&internal.DropSubscriptionCommand{
			Database:        proto.String(database),
			RetentionPolicy: proto.String(rp),
			Name:            proto.String(name),
		},
	)
}

// SetData overwrites the underlying data in the meta store.
func (c *Client) SetData(data *Data) error {
	return c.retryUntilExec(internal.Command_SetDataCommand, internal.E_SetDataCommand_Command,
		&internal.SetDataCommand{
			Data: data.marshal(),
		},
	)
}

// Data returns a clone of the underlying data in the meta store.
func (c *Client) Data() Data {
	c.mu.RLock()
	defer c.mu.RUnlock()
	d := c.cacheData.Clone()
	return *d
}

// WaitForDataChanged returns a channel that will get closed when
// the metastore data has changed.
func (c *Client) WaitForDataChanged() chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.changed
}

// Status returns the meta node status in the meta store.
func (c *Client) Status() (*MetaNodeStatus, error) {
	c.mu.RLock()
	if len(c.metaServers) == 0 {
		c.mu.RUnlock()
		return nil, ErrServiceUnavailable
	}
	server := c.metaServers[0]
	c.mu.RUnlock()
	url := c.url(server) + "/status"

	ns := &MetaNodeStatus{}
	if err := requestStatus(c.client, url, ns); err != nil {
		return nil, err
	}
	return ns, nil
}

// MarshalBinary returns a binary representation of the underlying data.
func (c *Client) MarshalBinary() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.MarshalBinary()
}

// WithLogger sets the logger for the client.
func (c *Client) WithLogger(log *zap.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger = log.With(zap.String("service", "metaclient"))
}

func (c *Client) index() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.Index
}

// retryUntilExec will attempt the command on each of the metaservers until it either succeeds or
// hits the max number of tries
func (c *Client) retryUntilExec(typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	c.mu.RLock()
	if len(c.metaServers) == 0 {
		c.mu.RUnlock()
		return ErrServiceUnavailable
	}
	c.mu.RUnlock()
	var err error
	var index uint64
	tries := 0
	currentServer := 0
	var redirectServer string

	for {
		c.mu.RLock()
		// exit if we're closed
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil
		default:
			// we're still open, continue on
		}
		c.mu.RUnlock()

		// build the url to hit the redirect server or the next metaserver
		var url string
		if redirectServer != "" {
			url = redirectServer
			redirectServer = ""
		} else {
			c.mu.RLock()
			if currentServer >= len(c.metaServers) {
				currentServer = 0
			}
			server := c.metaServers[currentServer]
			c.mu.RUnlock()

			url = fmt.Sprintf("://%s/execute", server)
			if c.config.MetaTLSEnabled {
				url = "https" + url
			} else {
				url = "http" + url
			}
		}

		index, err = c.exec(url, typ, desc, value)
		tries++
		currentServer++

		if err == nil {
			c.waitForIndex(index)
			return nil
		}

		if tries > maxRetries {
			return err
		}

		if e, ok := err.(errRedirect); ok {
			redirectServer = e.host
			continue
		}

		if _, ok := err.(errCommand); ok {
			return err
		}

		time.Sleep(errSleep)
	}
}

func (c *Client) exec(url string, typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) (index uint64, err error) {
	// Create command.
	cmd := &internal.Command{Type: &typ}
	if err := proto.SetExtension(cmd, desc, value); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return 0, err
	}

	resp, err := c.client.Post(url, "application/octet-stream", bytes.NewBuffer(b))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// read the response
	if resp.StatusCode == http.StatusTemporaryRedirect {
		return 0, errRedirect{host: resp.Header.Get("Location")}
	} else if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("meta service returned %s", resp.Status)
	}

	res := &internal.Response{}

	b, err = io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	if err := proto.Unmarshal(b, res); err != nil {
		return 0, err
	}
	es := res.GetError()
	if es != "" {
		return 0, errCommand{msg: es}
	}

	return res.GetIndex(), nil
}

func (c *Client) waitForIndex(idx uint64) {
	for {
		c.mu.RLock()
		if c.cacheData.Index >= idx {
			c.mu.RUnlock()
			return
		}
		ch := c.changed
		c.mu.RUnlock()
		<-ch
	}
}

func (c *Client) pollForUpdates() {
	for {
		data := c.retryUntilSnapshot(c.index())
		if data == nil {
			// this will only be nil if the client has been closed,
			// so we can exit out
			return
		}

		// update the data and notify of the change
		c.mu.Lock()
		idx := c.cacheData.Index
		c.cacheData = data
		c.updateAuthCache()
		c.updateNodeID()
		c.updateMetaServers()
		if idx < data.Index {
			close(c.changed)
			c.changed = make(chan struct{})
		}
		c.mu.Unlock()
	}
}

func (c *Client) getSnapshot(server string, index uint64) (*Data, error) {
	resp, err := c.client.Get(c.url(server) + fmt.Sprintf("?index=%d", index))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("meta server returned non-200: %s", resp.Status)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	data := &Data{}
	if err := data.UnmarshalBinary(b); err != nil {
		return nil, err
	}

	return data, nil
}

func (c *Client) url(server string) string {
	url := fmt.Sprintf("://%s", server)

	if c.config.MetaTLSEnabled {
		url = "https" + url
	} else {
		url = "http" + url
	}

	return url
}

func (c *Client) retryUntilSnapshot(idx uint64) *Data {
	var errPrint atomic.Value
	errPrint.Store(true)
	currentServer := 0
	for {
		// get the index to look from and the server to poll
		c.mu.RLock()

		// exit if we're closed
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil
		default:
			// we're still open, continue on
		}

		if len(c.metaServers) == 0 {
			c.mu.RUnlock()
			time.Sleep(errSleep)
			continue
		}

		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		server := c.metaServers[currentServer]
		c.mu.RUnlock()

		data, err := c.getSnapshot(server, idx)

		if err == nil {
			return data
		}

		if errPrint.Load().(bool) {
			c.logger.Info("Failure getting snapshot", zap.String("server", server), zap.Error(err))
			errPrint.Store(false)
			go func() {
				<-time.After(30 * errSleep)
				errPrint.Store(true)
			}()
		}
		time.Sleep(errSleep)

		currentServer++
	}
}

func (c *Client) updateAuthCache() {
	// copy cached user info for still-present users
	newCache := make(map[string]authUser, len(c.authCache))

	for _, userInfo := range c.cacheData.Users {
		if cached, ok := c.authCache[userInfo.Name]; ok {
			if cached.bhash == userInfo.Hash {
				newCache[userInfo.Name] = cached
			}
		}
	}

	c.authCache = newCache
}

func (c *Client) updateNodeID() {
	for _, n := range c.cacheData.DataNodes {
		if n.TCPAddr == c.tcpAddr {
			c.nodeID = n.ID
			return
		}
	}
	c.nodeID = 0
}

func (c *Client) updateMetaServers() {
	if !c.opened {
		c.logger.Info("Using client state dir", zap.String("path", c.path))
	}
	var metaServers []string
	for _, n := range c.cacheData.MetaNodes {
		metaServers = append(metaServers, n.Addr)
	}
	if !c.opened || !reflect.DeepEqual(c.metaServers, metaServers) {
		c.metaServers = metaServers
		if err := c.Save(); err != nil {
			c.logger.Error("Error saving meta servers", zap.Error(err))
		}
		if !c.opened {
			c.opened = true
			c.logger.Info("Opened client")
		}
	}
}

// Load loads the current client file from disk.
func (c *Client) Load() error {
	file := filepath.Join(c.path, clientFile)

	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	var o struct {
		MetaServers []string
	}

	if err = json.NewDecoder(f).Decode(&o); err != nil {
		return err
	}
	c.metaServers = o.MetaServers

	return nil
}

// Save saves the client file to disk and replace the existing one if present
func (c *Client) Save() error {
	file := filepath.Join(c.path, clientFile)
	tmpFile := file + ".tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}

	var o struct {
		MetaServers []string
	}
	o.MetaServers = c.metaServers

	if err = json.NewEncoder(f).Encode(&o); err != nil {
		f.Close()
		return err
	}

	if err = f.Close(); nil != err {
		return err
	}

	return os.Rename(tmpFile, file)
}

type errRedirect struct {
	host string
}

func (e errRedirect) Error() string {
	return fmt.Sprintf("redirect to %s", e.host)
}

type errCommand struct {
	msg string
}

func (e errCommand) Error() string {
	return e.msg
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
