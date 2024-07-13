package meta_test

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tcp"
)

func TestMetaService_CreateRemoveMetaNode(t *testing.T) {
	t.Parallel()

	metaServers := freePorts(4)
	raftPeers := freePorts(4)

	cfg1 := newConfig()
	cfg1.HTTPBindAddress = metaServers[0]
	cfg1.BindAddress = raftPeers[0]
	defer os.RemoveAll(cfg1.Dir)
	cfg2 := newConfig()
	cfg2.HTTPBindAddress = metaServers[1]
	cfg2.BindAddress = raftPeers[1]
	defer os.RemoveAll(cfg2.Dir)

	var wg sync.WaitGroup
	wg.Add(2)
	s1 := newService(cfg1)
	go func() {
		defer wg.Done()
		if err := s1.Open(); err != nil {
			t.Log(err)
			t.Fail()
			return
		}
	}()
	defer s1.Close()

	s2 := newService(cfg2)
	go func() {
		defer wg.Done()
		if err := s2.Open(); err != nil {
			t.Log(err)
			t.Fail()
			return
		}
	}()
	defer s2.Close()
	wg.Wait()

	if err := joinPeers(metaServers[0:2]); err != nil {
		t.Fatalf("error join peers")
	}

	cfg3 := newConfig()
	cfg3.HTTPBindAddress = metaServers[2]
	cfg3.BindAddress = raftPeers[2]
	defer os.RemoveAll(cfg3.Dir)

	s3 := newService(cfg3)
	if err := s3.Open(); err != nil {
		t.Fatal(err)
	}
	defer s3.Close()

	if err := joinPeers(metaServers[0:3]); err != nil {
		t.Fatalf("error join peers")
	}

	c1 := meta.NewClient(cfg3)
	c1.SetMetaServers(metaServers[0:3])
	if err := c1.Open(); err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	metaNodes := c1.MetaNodes()
	if len(metaNodes) != 3 {
		t.Fatalf("meta nodes wrong: %v", metaNodes)
	}

	c := meta.NewClient(cfg1)
	c.SetMetaServers([]string{s1.HTTPAddr()})
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.DeleteMetaNode(3); err != nil {
		t.Fatal(err)
	}

	metaNodes = c.MetaNodes()
	if len(metaNodes) != 2 {
		t.Fatalf("meta nodes wrong: %v", metaNodes)
	}

	cfg4 := newConfig()
	cfg4.HTTPBindAddress = metaServers[3]
	cfg4.BindAddress = raftPeers[3]
	defer os.RemoveAll(cfg4.Dir)

	s4 := newService(cfg4)
	if err := s4.Open(); err != nil {
		t.Fatal(err)
	}
	defer s4.Close()

	metaServers4 := []string{metaServers[0], metaServers[1], metaServers[3]}
	if err := joinPeers(metaServers4); err != nil {
		t.Fatalf("error join peers")
	}

	c2 := meta.NewClient(cfg4)
	c2.SetMetaServers(metaServers4)
	if err := c2.Open(); err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	metaNodes = c2.MetaNodes()
	if len(metaNodes) != 3 {
		t.Fatalf("meta nodes wrong: %v", metaNodes)
	}
}

// Ensure that if we attempt to create a database and the client
// is pointed at a server that isn't the leader, it automatically
// hits the leader and finishes the command
func TestMetaService_CommandAgainstNonLeader(t *testing.T) {
	t.Parallel()

	cfgs := make([]*meta.Config, 3)
	srvs := make([]*testService, 3)
	metaServers := freePorts(len(cfgs))

	var wg sync.WaitGroup
	wg.Add(len(cfgs))

	for i, _ := range cfgs {
		c := newConfig()
		c.HTTPBindAddress = metaServers[i]
		cfgs[i] = c

		srvs[i] = newService(c)
		go func(srv *testService) {
			defer wg.Done()
			if err := srv.Open(); err != nil {
				t.Log(err)
				t.Fail()
				return
			}
		}(srvs[i])
		defer srvs[i].Close()
		defer os.RemoveAll(c.Dir)
	}
	wg.Wait()

	if err := joinPeers(metaServers); err != nil {
		t.Fatalf("error join peers")
	}

	for i := range cfgs {
		c := meta.NewClient(cfgs[i])
		c.SetMetaServers([]string{metaServers[i]})
		if err := c.Open(); err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		metaNodes := c.MetaNodes()
		if len(metaNodes) != 3 {
			t.Fatalf("node %d - meta nodes wrong: %v", i, metaNodes)
		}

		if _, err := c.CreateDatabase(fmt.Sprintf("foo%d", i)); err != nil {
			t.Fatalf("node %d: %s", i, err)
		}

		if db := c.Database(fmt.Sprintf("foo%d", i)); db == nil {
			t.Fatalf("node %d: database foo wasn't created", i)
		}
	}
}

// Ensure that the client will fail over to another server if the leader goes
// down. Also ensure that the cluster will come back up successfully after restart
func TestMetaService_FailureAndRestartCluster(t *testing.T) {
	t.Parallel()

	cfgs := make([]*meta.Config, 3)
	srvs := make([]*testService, 3)
	metaServers := freePorts(len(cfgs))
	raftPeers := freePorts(len(cfgs))

	var swg sync.WaitGroup
	swg.Add(len(cfgs))
	for i, _ := range cfgs {
		c := newConfig()
		c.HTTPBindAddress = metaServers[i]
		c.BindAddress = raftPeers[i]
		cfgs[i] = c

		srvs[i] = newService(c)
		go func(i int, srv *testService) {
			defer swg.Done()
			if err := srv.Open(); err != nil {
				t.Logf("opening server %d", i)
				t.Log(err)
				t.Fail()
				return
			}
		}(i, srvs[i])

		defer srvs[i].Close()
		defer os.RemoveAll(c.Dir)
	}
	swg.Wait()

	if err := joinPeers(metaServers); err != nil {
		t.Fatalf("error join peers")
	}

	c := meta.NewClient(cfgs[0])
	c.SetMetaServers(metaServers)
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// check to see we were assigned a valid clusterID
	c1ID := c.ClusterID()
	if c1ID == 0 {
		t.Fatalf("invalid cluster id: %d", c1ID)
	}

	if _, err := c.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	if db := c.Database("foo"); db == nil {
		t.Fatalf("database foo wasn't created")
	}

	if err := srvs[0].Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := c.CreateDatabase("bar"); err != nil {
		t.Fatal(err)
	}

	if db := c.Database("bar"); db == nil {
		t.Fatalf("database bar wasn't created")
	}

	if err := srvs[1].Close(); err != nil {
		t.Fatal(err)
	}
	if err := srvs[2].Close(); err != nil {
		t.Fatal(err)
	}

	// give them a second to shut down
	time.Sleep(time.Second)

	// need to start them all at once so they can discover the bind addresses for raft
	var wg sync.WaitGroup
	wg.Add(len(cfgs))
	for i, cfg := range cfgs {
		srvs[i] = newService(cfg)
		go func(srv *testService) {
			if err := srv.Open(); err != nil {
				panic(err)
			}
			wg.Done()
		}(srvs[i])
		defer srvs[i].Close()
	}
	wg.Wait()
	time.Sleep(time.Second)

	c2 := meta.NewClient(cfgs[1])
	c2.SetMetaServers(metaServers)
	if err := c2.Open(); err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	c2ID := c2.ClusterID()
	if c1ID != c2ID {
		t.Fatalf("invalid cluster id. got: %d, exp: %d", c2ID, c1ID)
	}

	if db := c2.Database("bar"); db == nil {
		t.Fatalf("database bar wasn't created")
	}

	if _, err := c2.CreateDatabase("asdf"); err != nil {
		t.Fatal(err)
	}

	if db := c2.Database("asdf"); db == nil {
		t.Fatalf("database bar wasn't created")
	}
}

// Ensures that everything works after a host name change. This is
// skipped by default. To enable add hosts foobar and asdf to your
// /etc/hosts file and point those to 127.0.0.1
func TestMetaService_NameChangeSingleNode(t *testing.T) {
	t.Skip("not enabled")
	t.Parallel()

	cfg := newConfig()
	cfg.SingleServer = true
	defer os.RemoveAll(cfg.Dir)

	cfg.BindAddress = "foobar:0"
	cfg.HTTPBindAddress = "foobar:0"
	s := newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c := meta.NewClient(cfg)
	c.SetMetaServers([]string{s.HTTPAddr()})
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	s.Close()
	time.Sleep(time.Second)

	cfg.BindAddress = "asdf" + ":" + strings.Split(s.RaftAddr(), ":")[1]
	cfg.HTTPBindAddress = "asdf" + ":" + strings.Split(s.HTTPAddr(), ":")[1]
	s = newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c2 := meta.NewClient(cfg)
	c2.SetMetaServers([]string{s.HTTPAddr()})
	if err := c2.Open(); err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	db := c2.Database("foo")
	if db == nil {
		t.Fatal("database not found")
	}

	nodes := c2.MetaNodes()
	exp := []meta.NodeInfo{{ID: 1, Addr: cfg.HTTPBindAddress, TCPAddr: cfg.BindAddress}}

	time.Sleep(10 * time.Second)
	if !reflect.DeepEqual(nodes, exp) {
		t.Fatalf("nodes don't match: %v", nodes)
	}
}

func TestMetaService_CreateDataNode(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	exp := &meta.NodeInfo{
		ID:      2,
		Addr:    "foo:8380",
		TCPAddr: "bar:8381",
	}

	n, err := c.CreateDataNode(exp.Addr, exp.TCPAddr)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(n, exp) {
		t.Fatalf("data node attributes wrong: %v", n)
	}

	nodes := c.DataNodes()

	if !reflect.DeepEqual(nodes, []meta.NodeInfo{*exp}) {
		t.Fatalf("nodes wrong: %v", nodes)
	}
}

func TestMetaService_DropDataNode(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Dropping a data node with an invalid ID returns an error
	if err := c.DeleteDataNode(0); err == nil {
		t.Fatalf("Didn't get an error but expected %s", meta.ErrNodeNotFound)
	} else if err.Error() != meta.ErrNodeNotFound.Error() {
		t.Fatalf("got %v, expected %v", err, meta.ErrNodeNotFound)
	}

	// Create a couple of nodes.
	n1, err := c.CreateDataNode("foo:8180", "bar:8181")
	if err != nil {
		t.Fatal(err)
	}

	n2, err := c.CreateDataNode("foo:8280", "bar:8281")
	if err != nil {
		t.Fatal(err)
	}

	// Create a database and shard group. The default retention policy
	// means that the created shards should be replicated (owned) by
	// both the data nodes.
	if _, err := c.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	sg, err := c.CreateShardGroup("foo", "autogen", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// Dropping the first data server should result in that node ID
	// being removed as an owner of the shard.
	if err := c.DeleteDataNode(n1.ID); err != nil {
		t.Fatal(err)
	}

	// Retrieve updated shard group data from the Meta Store.
	rp, _ := c.RetentionPolicy("foo", "autogen")
	sg = &rp.ShardGroups[0]

	// The first data node should be removed as an owner of the shard on
	// the shard group
	if !reflect.DeepEqual(sg.Shards[0].Owners, []meta.ShardOwner{{n2.ID}}) {
		t.Errorf("owners for shard are %v, expected %v", sg.Shards[0].Owners, []meta.ShardOwner{{2}})
	}

	// The shard group should still be marked as active because it still
	// has a shard with owners.
	if sg.Deleted() {
		t.Error("shard group marked as deleted, but shouldn't be")
	}

	// Dropping the second data node will orphan the shard, but as
	// there won't be any shards left in the shard group, the shard
	// group will be deleted.
	if err := c.DeleteDataNode(n2.ID); err != nil {
		t.Fatal(err)
	}

	// Retrieve updated data.
	rp, _ = c.RetentionPolicy("foo", "autogen")
	sg = &rp.ShardGroups[0]

	if got, exp := sg.Deleted(), true; got != exp {
		t.Error("Shard group not marked as deleted")
	}
}

func TestMetaService_DropDataNode_Reassign(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create a couple of nodes.
	n1, err := c.CreateDataNode("foo:8180", "bar:8181")
	if err != nil {
		t.Fatal(err)
	}

	n2, err := c.CreateDataNode("foo:8280", "bar:8281")
	if err != nil {
		t.Fatal(err)
	}

	// Create a retention policy with a replica factor of 1.
	replicaN, duration := 1, time.Duration(0)
	rp := &meta.RetentionPolicySpec{
		Name:               "rp0",
		ReplicaN:           &replicaN,
		Duration:           &duration,
		ShardGroupDuration: 1 * time.Hour,
	}

	// Create a database using rp0
	if _, err := c.CreateDatabaseWithRetentionPolicy("foo", rp); err != nil {
		t.Fatal(err)
	}

	sg, err := c.CreateShardGroup("foo", "rp0", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// Dropping the first data server should result in the shard being
	// reassigned to the other node.
	if err := c.DeleteDataNode(n1.ID); err != nil {
		t.Fatal(err)
	}

	// Retrieve updated shard group data from the Meta Store.
	rpi, _ := c.RetentionPolicy("foo", "rp0")
	sg = &rpi.ShardGroups[0]

	// There should still be two shards.
	if got, exp := len(sg.Shards), 2; got != exp {
		t.Errorf("there are %d shards, but should be %d", got, exp)
	}

	// The second data node should be the owner of both shards.
	for _, s := range sg.Shards {
		if !reflect.DeepEqual(s.Owners, []meta.ShardOwner{{n2.ID}}) {
			t.Errorf("owners for shard are %v, expected %v", s.Owners, []meta.ShardOwner{{2}})
		}
	}

	// The shard group should not be marked as deleted because both
	// shards have an owner.
	if sg.Deleted() {
		t.Error("shard group marked as deleted, but shouldn't be")
	}
}

func TestMetaService_Ping(t *testing.T) {
	cfgs := make([]*meta.Config, 3)
	srvs := make([]*testService, 3)
	metaServers := freePorts(len(cfgs))

	var swg sync.WaitGroup
	swg.Add(len(cfgs))

	for i := range cfgs {
		c := newConfig()
		c.HTTPBindAddress = metaServers[i]
		cfgs[i] = c

		srvs[i] = newService(c)
		go func(i int, srv *testService) {
			defer swg.Done()
			if err := srv.Open(); err != nil {
				t.Logf("error opening server %d: %s", i, err)
				t.Fail()
				return
			}
		}(i, srvs[i])
		defer srvs[i].Close()
		defer os.RemoveAll(c.Dir)
	}
	swg.Wait()

	if err := joinPeers(metaServers); err != nil {
		t.Fatalf("error join peers")
	}

	c := meta.NewClient(cfgs[0])
	c.SetMetaServers(metaServers)
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Ping(false); err != nil {
		t.Fatalf("ping false all failed: %s", err)
	}
	if err := c.Ping(true); err != nil {
		t.Fatalf("ping false true failed: %s", err)
	}

	srvs[1].Close()
	// give the server time to close
	time.Sleep(time.Second)

	if err := c.Ping(false); err != nil {
		t.Fatalf("ping false some failed: %s", err)
	}

	if err := c.Ping(true); err == nil {
		t.Fatal("expected error on ping")
	}
}

func TestMetaService_AcquireLease(t *testing.T) {
	t.Parallel()

	cfg := newConfig()
	cfg.SingleServer = true
	defer os.RemoveAll(cfg.Dir)
	s := newService(cfg)
	if err := s.Open(); err != nil {
		panic(err)
	}
	defer s.Close()

	c1 := meta.NewClient(cfg)
	c1.SetMetaServers([]string{s.HTTPAddr()})
	c1.SetTCPAddr("bar1:8281")
	if err := c1.Open(); err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	c2 := meta.NewClient(cfg)
	c2.SetMetaServers([]string{s.HTTPAddr()})
	c2.SetTCPAddr("bar2:8281")
	if err := c2.Open(); err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	n1, err := c1.CreateDataNode("foo1:8180", "bar1:8281")
	if err != nil {
		t.Fatal(err)
	}

	n2, err := c2.CreateDataNode("foo2:8180", "bar2:8281")
	if err != nil {
		t.Fatal(err)
	}

	// Client 1 acquires a lease.  Should succeed.
	l, err := c1.AcquireLease("foo")
	if err != nil {
		t.Fatal(err)
	} else if l == nil {
		t.Fatal("expected *Lease")
	} else if l.Name != "foo" {
		t.Fatalf("lease name wrong: %s", l.Name)
	} else if l.Owner != n1.ID {
		t.Fatalf("owner ID wrong. exp %d got %d", n1.ID, l.Owner)
	}

	t.Logf("c1: %d, c2: %d", c1.NodeID(), c2.NodeID())
	// Client 2 attempts to acquire the same lease.  Should fail.
	l, err = c2.AcquireLease("foo")
	if err == nil {
		t.Fatal("expected to fail because another node owns the lease")
	}

	// Wait for Client 1's lease to expire.
	time.Sleep(1 * time.Second)

	// Client 2 retries to acquire the lease.  Should succeed this time.
	l, err = c2.AcquireLease("foo")
	if err != nil {
		t.Fatal(err)
	} else if l == nil {
		t.Fatal("expected *Lease")
	} else if l.Name != "foo" {
		t.Fatalf("lease name wrong: %s", l.Name)
	} else if l.Owner != n2.ID {
		t.Fatalf("owner ID wrong. exp %d got %d", n2.ID, l.Owner)
	}
}

// newServiceAndClient returns new data directory, *Service, and *Client or panics.
// Caller is responsible for deleting data dir and closing client.
func newServiceAndClient() (string, *testService, *meta.Client) {
	cfg := newConfig()
	cfg.SingleServer = true
	s := newService(cfg)
	if err := s.Open(); err != nil {
		panic(err)
	}

	c := newClient(cfg)

	return cfg.Dir, s, c
}

type testService struct {
	*meta.Service
	ln net.Listener
	ss bool
}

func (t *testService) Open() error {
	if t.ss {
		return t.Service.Open()
	} else {
		go func() {
			if err := t.Service.Open(); err != nil {
				panic(err)
			}
		}()
		time.Sleep(2 * time.Second)
		return nil
	}
}

func (t *testService) Close() error {
	if err := t.Service.Close(); err != nil {
		return err
	}
	return t.ln.Close()
}

func newService(cfg *meta.Config) *testService {
	// Open shared TCP connection.
	ln, err := net.Listen("tcp", cfg.BindAddress)
	if err != nil {
		panic(err)
	}

	// Multiplex listener.
	mux := tcp.NewMux()

	if err != nil {
		panic(err)
	}
	s := meta.NewService(cfg)
	s.RaftListener = mux.Listen(meta.MuxHeader)

	go mux.Serve(ln)

	return &testService{Service: s, ln: ln, ss: cfg.SingleServer}
}

func joinPeers(peers []string) error {
	if len(peers) == 0 {
		return errors.New("empty peers")
	}
	cOpts := &common.Options{
		BindAddr: peers[0],
	}
	for _, peer := range peers {
		client := common.NewHTTPClient(cOpts)
		defer client.Close()
		data := url.Values{"addr": {peer}}
		resp, err := client.PostForm("/join", data)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return meta.DecodeErrorResponse(resp.Body)
		}
	}

	for _, peer := range peers {
		cfg := newConfig()
		c := meta.NewClient(cfg)
		c.SetMetaServers([]string{peer})
		if err := c.Open(); err != nil {
			panic(fmt.Errorf("client open error: %s", err))
		}
		defer c.Close()

		timeout := time.Now().Add(10 * time.Second)
		for {
			metaNodes := c.MetaNodes()
			if len(metaNodes) == len(peers) {
				break
			}
			if time.Now().After(timeout) {
				panic(fmt.Errorf("node %s - meta nodes wrong: %v, timed out", peer, metaNodes))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

func freePort() string {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	return l.Addr().String()
}

func freePorts(i int) []string {
	var ports []string
	for j := 0; j < i; j++ {
		ports = append(ports, freePort())
	}
	return ports
}
