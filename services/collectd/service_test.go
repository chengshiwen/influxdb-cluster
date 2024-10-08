package collectd

import (
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/toml"
)

func TestService_OpenClose(t *testing.T) {
	service := NewTestService(1, time.Second, "split")

	// Closing a closed service is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	// Closing a closed service again is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Opening an already open service is fine.
	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Reopening a previously opened service is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Tidy up.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}
}

// Test that the service can read types DB files from a directory.
func TestService_Open_TypesDBDir(t *testing.T) {
	t.Parallel()

	// Make a temp dir to write types.db into.
	tmpDir, err := os.MkdirTemp(os.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Write types.db.
	if err := os.WriteFile(path.Join(tmpDir, "types.db"), []byte(typesDBText), 0777); err != nil {
		t.Fatal(err)
	}

	// Setup config to read all files in the temp dir.
	c := Config{
		BindAddress:   "127.0.0.1:0",
		Database:      "collectd_test",
		BatchSize:     1000,
		BatchDuration: toml.Duration(time.Second),
		TypesDB:       tmpDir,
	}

	s := &TestService{
		Config:     c,
		Service:    NewService(c),
		MetaClient: &internal.MetaClientMock{},
	}

	if testing.Verbose() {
		s.Service.WithLogger(logger.New(os.Stderr))
	}

	s.MetaClient.CreateDatabaseFn = func(name string) (*meta.DatabaseInfo, error) {
		return nil, nil
	}

	s.Service.PointsWriter = s
	s.Service.MetaClient = s.MetaClient

	if err := s.Service.Open(); err != nil {
		t.Fatal(err)
	}

	if err := s.Service.Close(); err != nil {
		t.Fatal(err)
	}
}

// Test that the service checks / creates the target database every time we
// try to write points.
func TestService_CreatesDatabase(t *testing.T) {
	t.Parallel()

	s := NewTestService(1, time.Second, "split")

	s.WritePointsFn = func(string, string, models.ConsistencyLevel, []models.Point) error {
		return nil
	}

	called := make(chan struct{})
	s.MetaClient.CreateDatabaseFn = func(name string) (*meta.DatabaseInfo, error) {
		if name != s.Config.Database {
			t.Errorf("\n\texp = %s\n\tgot = %s\n", s.Config.Database, name)
		}
		// Allow some time for the caller to return and the ready status to
		// be set.
		time.AfterFunc(10*time.Millisecond, func() { called <- struct{}{} })
		return nil, errors.New("an error")
	}

	if err := s.Service.Open(); err != nil {
		t.Fatal(err)
	}

	points, err := models.ParsePointsString(`cpu value=1`)
	if err != nil {
		t.Fatal(err)
	}

	s.Service.batcher.In() <- points[0] // Send a point.
	s.Service.batcher.Flush()
	select {
	case <-called:
		// OK
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatal("Service should have attempted to create database")
	}

	// ready status should not have been switched due to meta client error.
	s.Service.mu.RLock()
	ready := s.Service.ready
	s.Service.mu.RUnlock()

	if got, exp := ready, false; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// This time MC won't cause an error.
	s.MetaClient.CreateDatabaseFn = func(name string) (*meta.DatabaseInfo, error) {
		// Allow some time for the caller to return and the ready status to
		// be set.
		time.AfterFunc(10*time.Millisecond, func() { called <- struct{}{} })
		return nil, nil
	}

	s.Service.batcher.In() <- points[0] // Send a point.
	s.Service.batcher.Flush()
	select {
	case <-called:
		// OK
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatal("Service should have attempted to create database")
	}

	// ready status should not have been switched due to meta client error.
	s.Service.mu.RLock()
	ready = s.Service.ready
	s.Service.mu.RUnlock()

	if got, exp := ready, true; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	s.Service.Close()
}

// Test that the collectd service correctly batches points by BatchSize.
func TestService_BatchSize(t *testing.T) {
	t.Parallel()

	totalPoints := len(expPoints)

	// Batch sizes that totalTestPoints divide evenly by.
	batchSizes := []int{1, 2, 13}

	for _, batchSize := range batchSizes {
		func() {
			s := NewTestService(batchSize, time.Second, "split")

			pointCh := make(chan models.Point)
			s.WritePointsFn = func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
				if len(points) != batchSize {
					t.Errorf("\n\texp = %d\n\tgot = %d\n", batchSize, len(points))
				}

				for _, p := range points {
					pointCh <- p
				}
				return nil
			}

			if err := s.Service.Open(); err != nil {
				t.Fatal(err)
			}
			defer func() { t.Log("closing service"); s.Service.Close() }()

			// Get the address & port the service is listening on for collectd data.
			addr := s.Service.Addr()
			conn, err := net.Dial("udp", addr.String())
			if err != nil {
				t.Fatal(err)
			}

			// Send the test data to the service.
			if n, err := conn.Write(testData); err != nil {
				t.Fatal(err)
			} else if n != len(testData) {
				t.Fatalf("only sent %d of %d bytes", n, len(testData))
			}

			var points []models.Point
			timer := time.NewTimer(time.Second)
		Loop:
			for {
				timer.Reset(time.Second)
				select {
				case p := <-pointCh:
					points = append(points, p)
					if len(points) == totalPoints {
						break Loop
					}
				case <-timer.C:
					t.Logf("exp %d points, got %d", totalPoints, len(points))
					t.Fatal("timed out waiting for points from collectd service")
				}
			}

			if len(points) != totalPoints {
				t.Fatalf("exp %d points, got %d", totalPoints, len(points))
			}

			for i, exp := range expPoints {
				got := points[i].String()
				if got != exp {
					t.Fatalf("\n\texp = %s\n\tgot = %s\n", exp, got)
				}
			}
		}()
	}
}

// Test that the parse-multi-value-plugin config works properly.
// The other tests already verify the 'split' config, so this only runs the 'join' test.
func TestService_ParseMultiValuePlugin(t *testing.T) {
	t.Parallel()

	totalPoints := len(expPointsTupled)

	s := NewTestService(1, time.Second, "join")

	pointCh := make(chan models.Point, 1000)
	s.WritePointsFn = func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
		for _, p := range points {
			pointCh <- p
		}
		return nil
	}

	if err := s.Service.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { t.Log("closing service"); s.Service.Close() }()

	// Get the address & port the service is listening on for collectd data.
	addr := s.Service.Addr()
	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}

	// Send the test data to the service.
	if n, err := conn.Write(testData); err != nil {
		t.Fatal(err)
	} else if n != len(testData) {
		t.Fatalf("only sent %d of %d bytes", n, len(testData))
	}

	var points []models.Point

	timer := time.NewTimer(time.Second)
Loop:
	for {
		timer.Reset(time.Second)
		select {
		case p := <-pointCh:
			points = append(points, p)
			if len(points) == totalPoints {
				break Loop
			}
		case <-timer.C:
			t.Logf("exp %d points, got %d", totalPoints, len(points))
			t.Fatal("timed out waiting for points from collectd service")
		}
	}

	for i, exp := range expPointsTupled {
		got := points[i].String()
		if got != exp {
			t.Fatalf("\n\texp = %s\n\tgot = %s\n", exp, got)
		}
	}

}

// Test that the collectd service correctly batches points using BatchDuration.
func TestService_BatchDuration(t *testing.T) {
	t.Parallel()

	totalPoints := len(expPoints)

	s := NewTestService(5000, 250*time.Millisecond, "split")

	pointCh := make(chan models.Point, 1000)
	s.WritePointsFn = func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
		for _, p := range points {
			pointCh <- p
		}
		return nil
	}

	if err := s.Service.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { t.Log("closing service"); s.Service.Close() }()

	// Get the address & port the service is listening on for collectd data.
	addr := s.Service.Addr()
	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}

	// Send the test data to the service.
	if n, err := conn.Write(testData); err != nil {
		t.Fatal(err)
	} else if n != len(testData) {
		t.Fatalf("only sent %d of %d bytes", n, len(testData))
	}

	var points []models.Point
	timer := time.NewTimer(time.Second)
Loop:
	for {
		timer.Reset(time.Second)
		select {
		case p := <-pointCh:
			points = append(points, p)
			if len(points) == totalPoints {
				break Loop
			}
		case <-timer.C:
			t.Logf("exp %d points, got %d", totalPoints, len(points))
			t.Fatal("timed out waiting for points from collectd service")
		}
	}

	if len(points) != totalPoints {
		t.Fatalf("exp %d points, got %d", totalPoints, len(points))
	}

	for i, exp := range expPoints {
		got := points[i].String()
		if got != exp {
			t.Fatalf("\n\texp = %s\n\tgot = %s\n", exp, got)
		}
	}
}

type TestService struct {
	Service       *Service
	Config        Config
	MetaClient    *internal.MetaClientMock
	WritePointsFn func(string, string, models.ConsistencyLevel, []models.Point) error
}

func NewTestService(batchSize int, batchDuration time.Duration, parseOpt string) *TestService {
	c := Config{
		BindAddress:           "127.0.0.1:0",
		Database:              "collectd_test",
		BatchSize:             batchSize,
		BatchDuration:         toml.Duration(batchDuration),
		ParseMultiValuePlugin: parseOpt,
	}

	s := &TestService{
		Config:     c,
		Service:    NewService(c),
		MetaClient: &internal.MetaClientMock{},
	}

	s.MetaClient.CreateDatabaseFn = func(name string) (*meta.DatabaseInfo, error) {
		return nil, nil
	}

	s.Service.PointsWriter = s
	s.Service.MetaClient = s.MetaClient

	// Set the collectd types using test string.
	if err := s.Service.SetTypes(typesDBText); err != nil {
		panic(err)
	}

	if testing.Verbose() {
		s.Service.WithLogger(logger.New(os.Stderr))
	}

	return s
}

func (w *TestService) WritePointsPrivileged(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return w.WritePointsFn(database, retentionPolicy, consistencyLevel, points)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// Raw data sent by collectd, captured using Wireshark.
var testData = func() []byte {
	data := []string{
		"000000167066312d36322d3231302d39342d313733000001000c00000000544928ff0007000c0000000",
		"0000000050002000c656e74726f7079000004000c656e74726f7079000006000f000101000000000000",
		"7240000200086370750000030006310000040008637075000005000969646c65000006000f000100000",
		"0000000a674620005000977616974000006000f00010000000000000000000002000764660000030005",
		"00000400076466000005000d6c6976652d636f7700000600180002010100000000a090b641000000a0c",
		"b6a2742000200086370750000030006310000040008637075000005000e696e74657272757074000006",
		"000f00010000000000000000fe0005000c736f6674697271000006000f0001000000000000000000000",
		"20007646600000300050000040007646600000500096c69766500000600180002010100000000000000",
		"00000000e0ec972742000200086370750000030006310000040008637075000005000a737465616c000",
		"006000f00010000000000000000000003000632000005000975736572000006000f0001000000000000",
		"005f36000500096e696365000006000f0001000000000000000ad80002000e696e74657266616365000",
		"0030005000004000e69665f6f6374657473000005000b64756d6d793000000600180002000000000000",
		"00000000000000000000041a000200076466000004000764660000050008746d7000000600180002010",
		"1000000000000f240000000a0ea97274200020008637075000003000632000004000863707500000500",
		"0b73797374656d000006000f00010000000000000045d30002000e696e7465726661636500000300050",
		"00004000f69665f7061636b657473000005000b64756d6d793000000600180002000000000000000000",
		"00000000000000000f000200086370750000030006320000040008637075000005000969646c6500000",
		"6000f0001000000000000a66480000200076466000003000500000400076466000005000d72756e2d6c",
		"6f636b000006001800020101000000000000000000000000000054410002000e696e746572666163650",
		"00004000e69665f6572726f7273000005000b64756d6d79300000060018000200000000000000000000",
		"00000000000000000002000863707500000300063200000400086370750000050009776169740000060",
		"00f00010000000000000000000005000e696e74657272757074000006000f0001000000000000000132",
	}
	b, err := hex.DecodeString(strings.Join(data, ""))
	check(err)
	return b
}()

var expPoints = []string{
	"entropy_value,host=pf1-62-210-94-173,type=entropy value=288 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=idle value=10908770 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=wait value=0 1414080767000000000",
	"df_used,host=pf1-62-210-94-173,type=df,type_instance=live-cow value=378576896 1414080767000000000",
	"df_free,host=pf1-62-210-94-173,type=df,type_instance=live-cow value=50287988736 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=interrupt value=254 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=softirq value=0 1414080767000000000",
	"df_used,host=pf1-62-210-94-173,type=df,type_instance=live value=0 1414080767000000000",
	"df_free,host=pf1-62-210-94-173,type=df,type_instance=live value=50666565632 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=steal value=0 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=user value=24374 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=nice value=2776 1414080767000000000",
	"interface_rx,host=pf1-62-210-94-173,type=if_octets,type_instance=dummy0 value=0 1414080767000000000",
	"interface_tx,host=pf1-62-210-94-173,type=if_octets,type_instance=dummy0 value=1050 1414080767000000000",
	"df_used,host=pf1-62-210-94-173,type=df,type_instance=tmp value=73728 1414080767000000000",
	"df_free,host=pf1-62-210-94-173,type=df,type_instance=tmp value=50666491904 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=system value=17875 1414080767000000000",
	"interface_rx,host=pf1-62-210-94-173,type=if_packets,type_instance=dummy0 value=0 1414080767000000000",
	"interface_tx,host=pf1-62-210-94-173,type=if_packets,type_instance=dummy0 value=15 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=idle value=10904704 1414080767000000000",
	"df_used,host=pf1-62-210-94-173,type=df,type_instance=run-lock value=0 1414080767000000000",
	"df_free,host=pf1-62-210-94-173,type=df,type_instance=run-lock value=5242880 1414080767000000000",
	"interface_rx,host=pf1-62-210-94-173,type=if_errors,type_instance=dummy0 value=0 1414080767000000000",
	"interface_tx,host=pf1-62-210-94-173,type=if_errors,type_instance=dummy0 value=0 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=wait value=0 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=interrupt value=306 1414080767000000000",
}

var expPointsTupled = []string{
	"entropy,host=pf1-62-210-94-173,type=entropy value=288 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=idle value=10908770 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=wait value=0 1414080767000000000",
	"df,host=pf1-62-210-94-173,type=df,type_instance=live-cow free=50287988736,used=378576896 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=interrupt value=254 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=softirq value=0 1414080767000000000",
	"df,host=pf1-62-210-94-173,type=df,type_instance=live free=50666565632,used=0 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=steal value=0 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=user value=24374 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=nice value=2776 1414080767000000000",
	"interface,host=pf1-62-210-94-173,type=if_octets,type_instance=dummy0 rx=0,tx=1050 1414080767000000000",
	"df,host=pf1-62-210-94-173,type=df,type_instance=tmp free=50666491904,used=73728 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=system value=17875 1414080767000000000",
	"interface,host=pf1-62-210-94-173,type=if_packets,type_instance=dummy0 rx=0,tx=15 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=idle value=10904704 1414080767000000000",
	"df,host=pf1-62-210-94-173,type=df,type_instance=run-lock free=5242880,used=0 1414080767000000000",
	"interface,host=pf1-62-210-94-173,type=if_errors,type_instance=dummy0 rx=0,tx=0 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=wait value=0 1414080767000000000",
	"cpu,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=interrupt value=306 1414080767000000000",
}

// Taken from /usr/share/collectd/types.db on a Ubuntu system
var typesDBText = `
absolute		value:ABSOLUTE:0:U
apache_bytes		value:DERIVE:0:U
apache_connections	value:GAUGE:0:65535
apache_idle_workers	value:GAUGE:0:65535
apache_requests		value:DERIVE:0:U
apache_scoreboard	value:GAUGE:0:65535
ath_nodes		value:GAUGE:0:65535
ath_stat		value:DERIVE:0:U
backends		value:GAUGE:0:65535
bitrate			value:GAUGE:0:4294967295
bytes			value:GAUGE:0:U
cache_eviction		value:DERIVE:0:U
cache_operation		value:DERIVE:0:U
cache_ratio		value:GAUGE:0:100
cache_result		value:DERIVE:0:U
cache_size		value:GAUGE:0:4294967295
charge			value:GAUGE:0:U
compression_ratio	value:GAUGE:0:2
compression		uncompressed:DERIVE:0:U, compressed:DERIVE:0:U
connections		value:DERIVE:0:U
conntrack		value:GAUGE:0:4294967295
contextswitch		value:DERIVE:0:U
counter			value:COUNTER:U:U
cpufreq			value:GAUGE:0:U
cpu			value:DERIVE:0:U
current_connections	value:GAUGE:0:U
current_sessions	value:GAUGE:0:U
current			value:GAUGE:U:U
delay			value:GAUGE:-1000000:1000000
derive			value:DERIVE:0:U
df_complex		value:GAUGE:0:U
df_inodes		value:GAUGE:0:U
df			used:GAUGE:0:1125899906842623, free:GAUGE:0:1125899906842623
disk_latency		read:GAUGE:0:U, write:GAUGE:0:U
disk_merged		read:DERIVE:0:U, write:DERIVE:0:U
disk_octets		read:DERIVE:0:U, write:DERIVE:0:U
disk_ops_complex	value:DERIVE:0:U
disk_ops		read:DERIVE:0:U, write:DERIVE:0:U
disk_time		read:DERIVE:0:U, write:DERIVE:0:U
dns_answer		value:DERIVE:0:U
dns_notify		value:DERIVE:0:U
dns_octets		queries:DERIVE:0:U, responses:DERIVE:0:U
dns_opcode		value:DERIVE:0:U
dns_qtype_cached	value:GAUGE:0:4294967295
dns_qtype		value:DERIVE:0:U
dns_query		value:DERIVE:0:U
dns_question		value:DERIVE:0:U
dns_rcode		value:DERIVE:0:U
dns_reject		value:DERIVE:0:U
dns_request		value:DERIVE:0:U
dns_resolver		value:DERIVE:0:U
dns_response		value:DERIVE:0:U
dns_transfer		value:DERIVE:0:U
dns_update		value:DERIVE:0:U
dns_zops		value:DERIVE:0:U
duration		seconds:GAUGE:0:U
email_check		value:GAUGE:0:U
email_count		value:GAUGE:0:U
email_size		value:GAUGE:0:U
entropy			value:GAUGE:0:4294967295
fanspeed		value:GAUGE:0:U
file_size		value:GAUGE:0:U
files			value:GAUGE:0:U
fork_rate		value:DERIVE:0:U
frequency_offset	value:GAUGE:-1000000:1000000
frequency		value:GAUGE:0:U
fscache_stat		value:DERIVE:0:U
gauge			value:GAUGE:U:U
hash_collisions		value:DERIVE:0:U
http_request_methods	value:DERIVE:0:U
http_requests		value:DERIVE:0:U
http_response_codes	value:DERIVE:0:U
humidity		value:GAUGE:0:100
if_collisions		value:DERIVE:0:U
if_dropped		rx:DERIVE:0:U, tx:DERIVE:0:U
if_errors		rx:DERIVE:0:U, tx:DERIVE:0:U
if_multicast		value:DERIVE:0:U
if_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
if_packets		rx:DERIVE:0:U, tx:DERIVE:0:U
if_rx_errors		value:DERIVE:0:U
if_rx_octets		value:DERIVE:0:U
if_tx_errors		value:DERIVE:0:U
if_tx_octets		value:DERIVE:0:U
invocations		value:DERIVE:0:U
io_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
io_packets		rx:DERIVE:0:U, tx:DERIVE:0:U
ipt_bytes		value:DERIVE:0:U
ipt_packets		value:DERIVE:0:U
irq			value:DERIVE:0:U
latency			value:GAUGE:0:U
links			value:GAUGE:0:U
load			shortterm:GAUGE:0:5000, midterm:GAUGE:0:5000, longterm:GAUGE:0:5000
md_disks		value:GAUGE:0:U
memcached_command	value:DERIVE:0:U
memcached_connections	value:GAUGE:0:U
memcached_items		value:GAUGE:0:U
memcached_octets	rx:DERIVE:0:U, tx:DERIVE:0:U
memcached_ops		value:DERIVE:0:U
memory			value:GAUGE:0:281474976710656
multimeter		value:GAUGE:U:U
mutex_operations	value:DERIVE:0:U
mysql_commands		value:DERIVE:0:U
mysql_handler		value:DERIVE:0:U
mysql_locks		value:DERIVE:0:U
mysql_log_position	value:DERIVE:0:U
mysql_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
nfs_procedure		value:DERIVE:0:U
nginx_connections	value:GAUGE:0:U
nginx_requests		value:DERIVE:0:U
node_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
node_rssi		value:GAUGE:0:255
node_stat		value:DERIVE:0:U
node_tx_rate		value:GAUGE:0:127
objects			value:GAUGE:0:U
operations		value:DERIVE:0:U
percent			value:GAUGE:0:100.1
percent_bytes		value:GAUGE:0:100.1
percent_inodes		value:GAUGE:0:100.1
pf_counters		value:DERIVE:0:U
pf_limits		value:DERIVE:0:U
pf_source		value:DERIVE:0:U
pf_states		value:GAUGE:0:U
pf_state		value:DERIVE:0:U
pg_blks			value:DERIVE:0:U
pg_db_size		value:GAUGE:0:U
pg_n_tup_c		value:DERIVE:0:U
pg_n_tup_g		value:GAUGE:0:U
pg_numbackends		value:GAUGE:0:U
pg_scan			value:DERIVE:0:U
pg_xact			value:DERIVE:0:U
ping_droprate		value:GAUGE:0:100
ping_stddev		value:GAUGE:0:65535
ping			value:GAUGE:0:65535
players			value:GAUGE:0:1000000
power			value:GAUGE:0:U
protocol_counter	value:DERIVE:0:U
ps_code			value:GAUGE:0:9223372036854775807
ps_count		processes:GAUGE:0:1000000, threads:GAUGE:0:1000000
ps_cputime		user:DERIVE:0:U, syst:DERIVE:0:U
ps_data			value:GAUGE:0:9223372036854775807
ps_disk_octets		read:DERIVE:0:U, write:DERIVE:0:U
ps_disk_ops		read:DERIVE:0:U, write:DERIVE:0:U
ps_pagefaults		minflt:DERIVE:0:U, majflt:DERIVE:0:U
ps_rss			value:GAUGE:0:9223372036854775807
ps_stacksize		value:GAUGE:0:9223372036854775807
ps_state		value:GAUGE:0:65535
ps_vm			value:GAUGE:0:9223372036854775807
queue_length		value:GAUGE:0:U
records			value:GAUGE:0:U
requests		value:GAUGE:0:U
response_time		value:GAUGE:0:U
response_code		value:GAUGE:0:U
route_etx		value:GAUGE:0:U
route_metric		value:GAUGE:0:U
routes			value:GAUGE:0:U
serial_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
signal_noise		value:GAUGE:U:0
signal_power		value:GAUGE:U:0
signal_quality		value:GAUGE:0:U
snr			value:GAUGE:0:U
spam_check		value:GAUGE:0:U
spam_score		value:GAUGE:U:U
spl			value:GAUGE:U:U
swap_io			value:DERIVE:0:U
swap			value:GAUGE:0:1099511627776
tcp_connections		value:GAUGE:0:4294967295
temperature		value:GAUGE:U:U
threads			value:GAUGE:0:U
time_dispersion		value:GAUGE:-1000000:1000000
timeleft		value:GAUGE:0:U
time_offset		value:GAUGE:-1000000:1000000
total_bytes		value:DERIVE:0:U
total_connections	value:DERIVE:0:U
total_objects		value:DERIVE:0:U
total_operations	value:DERIVE:0:U
total_requests		value:DERIVE:0:U
total_sessions		value:DERIVE:0:U
total_threads		value:DERIVE:0:U
total_time_in_ms	value:DERIVE:0:U
total_values		value:DERIVE:0:U
uptime			value:GAUGE:0:4294967295
users			value:GAUGE:0:65535
vcl			value:GAUGE:0:65535
vcpu			value:GAUGE:0:U
virt_cpu_total		value:DERIVE:0:U
virt_vcpu		value:DERIVE:0:U
vmpage_action		value:DERIVE:0:U
vmpage_faults		minflt:DERIVE:0:U, majflt:DERIVE:0:U
vmpage_io		in:DERIVE:0:U, out:DERIVE:0:U
vmpage_number		value:GAUGE:0:4294967295
volatile_changes	value:GAUGE:0:U
voltage_threshold	value:GAUGE:U:U, threshold:GAUGE:U:U
voltage			value:GAUGE:U:U
vs_memory		value:GAUGE:0:9223372036854775807
vs_processes		value:GAUGE:0:65535
vs_threads		value:GAUGE:0:65535
#
# Legacy types
# (required for the v5 upgrade target)
#
arc_counts		demand_data:COUNTER:0:U, demand_metadata:COUNTER:0:U, prefetch_data:COUNTER:0:U, prefetch_metadata:COUNTER:0:U
arc_l2_bytes		read:COUNTER:0:U, write:COUNTER:0:U
arc_l2_size		value:GAUGE:0:U
arc_ratio		value:GAUGE:0:U
arc_size		current:GAUGE:0:U, target:GAUGE:0:U, minlimit:GAUGE:0:U, maxlimit:GAUGE:0:U
mysql_qcache		hits:COUNTER:0:U, inserts:COUNTER:0:U, not_cached:COUNTER:0:U, lowmem_prunes:COUNTER:0:U, queries_in_cache:GAUGE:0:U
mysql_threads		running:GAUGE:0:U, connected:GAUGE:0:U, cached:GAUGE:0:U, created:COUNTER:0:U
`
