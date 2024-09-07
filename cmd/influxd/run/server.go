package run

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/flux/control"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/ae"
	"github.com/influxdata/influxdb/services/announcer"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/continuous_querier"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/hh"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/influxdb/services/precreator"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/services/subscriber"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	client "github.com/influxdata/usage-client/v1"
	"go.uber.org/zap"

	// Initialize the engine package
	_ "github.com/influxdata/influxdb/tsdb/engine"
	// Initialize the index package
	_ "github.com/influxdata/influxdb/tsdb/index"
)

var startTime time.Time

func init() {
	startTime = time.Now().UTC()
}

// BuildInfo represents the build details for the server code.
type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
	Time    string
}

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	buildInfo BuildInfo

	err     chan error
	closing chan struct{}

	BindAddress string
	Listener    net.Listener

	Logger *zap.Logger

	MetaClient *meta.Client

	TSDBStore     *tsdb.Store
	ClusterStore  *coordinator.ClusterTSDBStore
	QueryExecutor *query.Executor
	MetaExecutor  *coordinator.MetaExecutor
	PointsWriter  *coordinator.PointsWriter
	ShardWriter   *coordinator.ShardWriter
	HintedHandoff *hh.Service
	Subscriber    *subscriber.Service

	Services []Service

	// These references are required for the tcp muxer.
	CoordinatorService *coordinator.Service
	SnapshotterService *snapshotter.Service

	Monitor *monitor.Monitor

	// Server reporting and registration
	reportingDisabled bool

	// Profiling
	CPUProfile            string
	CPUProfileWriteCloser io.WriteCloser
	MemProfile            string
	MemProfileWriteCloser io.WriteCloser

	// httpAPIAddr is the host:port combination for the main HTTP API for querying and writing data
	httpAPIAddr string

	// httpUseTLS specifies if we should use a TLS connection to the http servers
	httpUseTLS bool

	// tcpAddr is the host:port combination for the TCP listener that services mux onto
	tcpAddr string

	config *Config
}

// updateTLSConfig stores with into the tls config pointed at by into but only if with is not nil
// and into is nil. Think of it as setting the default value.
func updateTLSConfig(into **tls.Config, with *tls.Config) {
	if with != nil && into != nil && *into == nil {
		*into = with
	}
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, buildInfo *BuildInfo) (*Server, error) {
	// First grab the base tls config we will use for all clients and servers
	tlsConfig, err := c.TLS.Parse()
	if err != nil {
		return nil, fmt.Errorf("tls configuration: %v", err)
	}

	// Update the TLS values on each of the configs to be the parsed one if
	// not already specified (set the default).
	updateTLSConfig(&c.Coordinator.TLS, tlsConfig)
	updateTLSConfig(&c.HTTPD.TLS, tlsConfig)
	updateTLSConfig(&c.Subscriber.TLS, tlsConfig)
	for i := range c.OpenTSDBInputs {
		updateTLSConfig(&c.OpenTSDBInputs[i].TLS, tlsConfig)
	}

	// We need to ensure that a meta directory always exists.
	if err := os.MkdirAll(c.Meta.Dir, 0777); err != nil {
		return nil, fmt.Errorf("mkdir all: %s", err)
	}

	// In 0.10.0 bind-address got moved to the top level. Check
	// The old location to keep things backwards compatible
	bind := c.BindAddress

	s := &Server{
		buildInfo: *buildInfo,
		err:       make(chan error),
		closing:   make(chan struct{}),

		BindAddress: bind,

		Logger: logger.New(os.Stderr),

		MetaClient: meta.NewClient(c.Meta),

		reportingDisabled: c.ReportingDisabled,

		httpAPIAddr: c.HTTPD.BindAddress,
		httpUseTLS:  c.HTTPD.HTTPSEnabled,
		tcpAddr:     bind,

		config: c,
	}
	s.Monitor = monitor.New(s, c.Monitor)
	s.config.registerDiagnostics(s.Monitor)

	// Set tcp addr on the client.
	s.MetaClient.SetTCPAddr(s.TCPAddr())

	s.TSDBStore = tsdb.NewStore(c.Data.Dir)
	s.TSDBStore.EngineOptions.Config = c.Data

	// Copy TSDB configuration.
	s.TSDBStore.EngineOptions.EngineVersion = c.Data.Engine
	s.TSDBStore.EngineOptions.IndexVersion = c.Data.Index

	// Create TLS client config
	tlsClientConfig := c.Coordinator.TLSClientConfig()

	// Set the shard writer
	s.ShardWriter = coordinator.NewShardWriter(time.Duration(c.Coordinator.WriteTimeout), time.Duration(c.Coordinator.DialTimeout),
		time.Duration(c.Coordinator.PoolMaxIdleTime), c.Coordinator.PoolMaxIdleStreams)
	s.ShardWriter.TLSConfig = tlsClientConfig

	// Create the hinted handoff service
	s.HintedHandoff = hh.NewService(c.HintedHandoff, s.ShardWriter)
	s.HintedHandoff.Monitor = s.Monitor

	// Create the Subscriber service
	s.Subscriber = subscriber.NewService(c.Subscriber)

	// Initialize points writer.
	s.PointsWriter = coordinator.NewPointsWriter()
	s.PointsWriter.AllowOutOfOrderWrites = c.Coordinator.AllowOutOfOrderWrites
	s.PointsWriter.WriteTimeout = time.Duration(c.Coordinator.WriteTimeout)
	s.PointsWriter.TSDBStore = s.TSDBStore
	s.PointsWriter.ShardWriter = s.ShardWriter
	s.PointsWriter.HintedHandoff = s.HintedHandoff
	s.PointsWriter.Subscriber = s.Subscriber

	// Initialize meta executor.
	s.MetaExecutor = coordinator.NewMetaExecutor(time.Duration(c.Coordinator.ShardReaderTimeout), time.Duration(c.Coordinator.DialTimeout),
		time.Duration(c.Coordinator.PoolMaxIdleTime), c.Coordinator.PoolMaxIdleStreams)
	s.MetaExecutor.MetaClient = s.MetaClient
	s.MetaExecutor.TLSConfig = tlsClientConfig

	// Initialize cluster TSDB store.
	s.ClusterStore = &coordinator.ClusterTSDBStore{Store: s.TSDBStore, MetaExecutor: s.MetaExecutor}

	// Initialize query executor.
	s.QueryExecutor = query.NewExecutor()
	s.QueryExecutor.StatementExecutor = &coordinator.StatementExecutor{
		MetaClient:  s.MetaClient,
		TaskManager: &coordinator.ClusterTaskManager{TaskManager: s.QueryExecutor.TaskManager, MetaExecutor: s.MetaExecutor},
		TSDBStore:   s.ClusterStore,
		ShardMapper: &coordinator.ClusterShardMapper{
			MetaClient:   s.MetaClient,
			TSDBStore:    s.TSDBStore,
			MetaExecutor: s.MetaExecutor,
		},
		StrictErrorHandling: s.TSDBStore.EngineOptions.Config.StrictErrorHandling,
		Monitor:             s.Monitor,
		PointsWriter:        s.PointsWriter,
		MaxSelectPointN:     c.Coordinator.MaxSelectPointN,
		MaxSelectSeriesN:    c.Coordinator.MaxSelectSeriesN,
		MaxSelectBucketsN:   c.Coordinator.MaxSelectBucketsN,
	}
	s.QueryExecutor.TaskManager.QueryTimeout = time.Duration(c.Coordinator.QueryTimeout)
	s.QueryExecutor.TaskManager.LogQueriesAfter = time.Duration(c.Coordinator.LogQueriesAfter)
	s.QueryExecutor.TaskManager.MaxConcurrentQueries = c.Coordinator.MaxConcurrentQueries
	s.QueryExecutor.TaskManager.LogTimedoutQueries = c.Coordinator.LogTimedOutQueries

	// Initialize the monitor
	s.Monitor.Version = s.buildInfo.Version
	s.Monitor.Commit = s.buildInfo.Commit
	s.Monitor.Branch = s.buildInfo.Branch
	s.Monitor.BuildTime = s.buildInfo.Time
	s.Monitor.PointsWriter = (*monitorPointsWriter)(s.PointsWriter)
	return s, nil
}

// Statistics returns statistics for the services running in the Server.
func (s *Server) Statistics(tags map[string]string) []models.Statistic {
	var statistics []models.Statistic
	statistics = append(statistics, s.QueryExecutor.Statistics(tags)...)
	statistics = append(statistics, s.TSDBStore.Statistics(tags)...)
	statistics = append(statistics, s.PointsWriter.Statistics(tags)...)
	statistics = append(statistics, s.HintedHandoff.Statistics(tags)...)
	statistics = append(statistics, s.Subscriber.Statistics(tags)...)
	for _, srv := range s.Services {
		if m, ok := srv.(monitor.Reporter); ok {
			statistics = append(statistics, m.Statistics(tags)...)
		}
	}
	return statistics
}

func (s *Server) appendCoordinatorService(c coordinator.Config) {
	srv := coordinator.NewService(c)
	srv.TSDBStore = s.TSDBStore
	srv.MetaClient = s.MetaClient
	srv.HintedHandoff = s.HintedHandoff
	srv.TaskManager = s.QueryExecutor.TaskManager
	srv.Store = storage.NewStore(s.TSDBStore, s.MetaClient)
	srv.Monitor = s.Monitor
	srv.Server = s
	s.Services = append(s.Services, srv)
	s.CoordinatorService = srv
}

func (s *Server) appendSnapshotterService() {
	srv := snapshotter.NewService()
	srv.TSDBStore = s.TSDBStore
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	s.SnapshotterService = srv
}

func (s *Server) appendMonitorService() {
	s.Services = append(s.Services, s.Monitor)
}

func (s *Server) appendAnnouncerService(c *meta.Config) {
	srv := announcer.NewService(c)
	srv.Server = s
	srv.MetaClient = s.MetaClient
	srv.Version = s.buildInfo.Version
	s.Services = append(s.Services, srv)
}

func (s *Server) appendRetentionPolicyService(c retention.Config) {
	if !c.Enabled {
		return
	}
	srv := retention.NewService(c)
	srv.MetaClient = s.MetaClient
	srv.TSDBStore = s.TSDBStore
	s.Services = append(s.Services, srv)
}

func (s *Server) appendAntiEntropyService(c ae.Config) {
	if !c.Enabled {
		return
	}
	srv := ae.NewService(c)
	srv.MetaClient = s.MetaClient
	srv.TSDBStore = s.TSDBStore
	s.Services = append(s.Services, srv)
}

func (s *Server) appendHTTPDService(c httpd.Config) {
	if !c.Enabled {
		return
	}
	srv := httpd.NewService(c)
	srv.Handler.MetaClient = s.MetaClient
	authorizer := meta.NewQueryAuthorizer(s.MetaClient)
	srv.Handler.QueryAuthorizer = authorizer
	srv.Handler.WriteAuthorizer = meta.NewWriteAuthorizer(s.MetaClient)
	srv.Handler.QueryExecutor = s.QueryExecutor
	srv.Handler.Monitor = s.Monitor
	srv.Handler.PointsWriter = s.PointsWriter
	srv.Handler.Version = s.buildInfo.Version
	srv.Handler.BuildType = "OSS"
	ss := storage.NewClusterStore(s.ClusterStore, s.MetaClient, s.MetaExecutor)
	srv.Handler.Store = ss
	if s.config.HTTPD.FluxEnabled {
		srv.Handler.Controller = control.NewController(s.MetaClient, reads.NewReader(ss), authorizer, c.AuthEnabled, s.Logger)
	}

	s.Services = append(s.Services, srv)
}

func (s *Server) appendCollectdService(c collectd.Config) {
	if !c.Enabled {
		return
	}
	srv := collectd.NewService(c)
	srv.MetaClient = s.MetaClient
	srv.PointsWriter = s.PointsWriter
	s.Services = append(s.Services, srv)
}

func (s *Server) appendOpenTSDBService(c opentsdb.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := opentsdb.NewService(c)
	if err != nil {
		return err
	}
	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendGraphiteService(c graphite.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := graphite.NewService(c)
	if err != nil {
		return err
	}

	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.MetaClient
	srv.Monitor = s.Monitor
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendPrecreatorService(c precreator.Config) error {
	if !c.Enabled {
		return nil
	}
	srv := precreator.NewService(c)
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendUDPService(c udp.Config) {
	if !c.Enabled {
		return
	}
	srv := udp.NewService(c)
	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
}

func (s *Server) appendContinuousQueryService(c continuous_querier.Config) {
	if !c.Enabled {
		return
	}
	srv := continuous_querier.NewService(c)
	srv.MetaClient = s.MetaClient
	srv.QueryExecutor = s.QueryExecutor
	srv.Monitor = s.Monitor
	s.Services = append(s.Services, srv)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Start profiling if requested.
	if err := s.startProfile(); err != nil {
		return err
	}

	// Open shared TCP connection.
	tlsConfig, err := s.config.Coordinator.TLSConfig()
	if err != nil {
		return fmt.Errorf("tls config: %s", err)
	}
	ln, err := tcp.ListenTLS("tcp", s.BindAddress, tlsConfig)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	// Multiplex listener.
	mux := tcp.NewMux()
	go mux.Serve(ln)

	// Append services.
	s.appendMonitorService()
	s.appendCoordinatorService(s.config.Coordinator)
	s.appendPrecreatorService(s.config.Precreator)
	s.appendSnapshotterService()
	s.appendContinuousQueryService(s.config.ContinuousQuery)
	s.appendHTTPDService(s.config.HTTPD)
	s.appendAnnouncerService(s.config.Meta)
	s.appendRetentionPolicyService(s.config.Retention)
	s.appendAntiEntropyService(s.config.AntiEntropy)
	for _, i := range s.config.GraphiteInputs {
		if err := s.appendGraphiteService(i); err != nil {
			return err
		}
	}
	for _, i := range s.config.CollectdInputs {
		s.appendCollectdService(i)
	}
	for _, i := range s.config.OpenTSDBInputs {
		if err := s.appendOpenTSDBService(i); err != nil {
			return err
		}
	}
	for _, i := range s.config.UDPInputs {
		s.appendUDPService(i)
	}

	s.ShardWriter.MetaClient = s.MetaClient
	s.HintedHandoff.MetaClient = s.MetaClient
	s.Subscriber.MetaClient = s.MetaClient
	s.PointsWriter.MetaClient = s.MetaClient
	s.Monitor.MetaClient = s.MetaClient

	s.CoordinatorService.Listener = mux.Listen(coordinator.MuxHeader)
	s.SnapshotterService.Listener = mux.Listen(snapshotter.MuxHeader)
	s.CoordinatorService.DefaultListener = mux.DefaultListener()

	// Configure logging for all services and clients.
	if s.config.Meta.LoggingEnabled {
		s.MetaClient.WithLogger(s.Logger)
	}
	s.TSDBStore.WithLogger(s.Logger)
	if s.config.Data.QueryLogEnabled {
		s.QueryExecutor.WithLogger(s.Logger)
	} else if s.config.Coordinator.LogQueriesAfter > 0 || s.config.Coordinator.LogTimedOutQueries {
		// If we need to do any logging, add a logger.
		// The TaskManager properly handles both of the above configs
		// so it only logs as is appropriate.
		s.QueryExecutor.TaskManager.Logger = s.Logger
	}
	s.PointsWriter.WithLogger(s.Logger)
	s.HintedHandoff.WithLogger(s.Logger)
	s.Subscriber.WithLogger(s.Logger)
	for _, svc := range s.Services {
		svc.WithLogger(s.Logger)
	}

	// Open TSDB store.
	if err := s.TSDBStore.Open(); err != nil {
		return fmt.Errorf("open tsdb store: %s", err)
	}

	// Open the meta client.
	if err := s.MetaClient.Open(); err != nil {
		return err
	}

	// Open the hinted handoff service
	if err := s.HintedHandoff.Open(); err != nil {
		return fmt.Errorf("open hinted handoff: %s", err)
	}

	// Open the subscriber service
	if err := s.Subscriber.Open(); err != nil {
		return fmt.Errorf("open subscriber: %s", err)
	}

	// Open the points writer service
	if err := s.PointsWriter.Open(); err != nil {
		return fmt.Errorf("open points writer: %s", err)
	}

	s.PointsWriter.AddWriteSubscriber(s.Subscriber.Points())

	for _, service := range s.Services {
		if err := service.Open(); err != nil {
			return fmt.Errorf("open service: %s", err)
		}
	}

	// Start the reporting service, if not disabled.
	if !s.reportingDisabled {
		go s.startServerReporting()
	}

	return nil
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	s.stopProfile()

	// Close the listener first to stop any new connections
	if s.Listener != nil {
		s.Listener.Close()
	}

	// Close services to allow any inflight requests to complete
	// and prevent new requests from being accepted.
	for _, service := range s.Services {
		service.Close()
	}

	s.config.deregisterDiagnostics(s.Monitor)

	if s.ShardWriter != nil {
		s.ShardWriter.Close()
	}

	if s.PointsWriter != nil {
		s.PointsWriter.Close()
	}

	if s.HintedHandoff != nil {
		s.HintedHandoff.Close()
	}

	if s.MetaExecutor != nil {
		s.MetaExecutor.Close()
	}

	if s.QueryExecutor != nil {
		s.QueryExecutor.Close()
	}

	// Close the TSDBStore, no more reads or writes at this point
	if s.TSDBStore != nil {
		s.TSDBStore.Close()
	}

	if s.Subscriber != nil {
		s.Subscriber.Close()
	}

	if s.MetaClient != nil {
		s.MetaClient.Close()
	}

	close(s.closing)
	return nil
}

// Reset resets the data stores and all services.
func (s *Server) Reset() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.RemoveAll(s.config.Data.Dir); err != nil {
		return fmt.Errorf("remove all: %s", err)
	}
	if err := os.RemoveAll(s.config.Data.WALDir); err != nil {
		return fmt.Errorf("remove all: %s", err)
	}
	if err := os.RemoveAll(s.config.HintedHandoff.Dir); err != nil {
		return fmt.Errorf("remove all: %s", err)
	}

	svr, err := NewServer(s.config, &s.buildInfo)
	if err != nil {
		return fmt.Errorf("create server: %s", err)
	}
	close(svr.err)
	s.closing = svr.closing
	s.MetaClient = svr.MetaClient
	s.TSDBStore = svr.TSDBStore
	s.ClusterStore = svr.ClusterStore
	s.QueryExecutor = svr.QueryExecutor
	s.MetaExecutor = svr.MetaExecutor
	s.PointsWriter = svr.PointsWriter
	s.ShardWriter = svr.ShardWriter
	s.HintedHandoff = svr.HintedHandoff
	s.Subscriber = svr.Subscriber
	s.Services = svr.Services
	s.CoordinatorService = svr.CoordinatorService
	s.SnapshotterService = svr.SnapshotterService
	s.Monitor = svr.Monitor

	if err = s.Open(); err != nil {
		return fmt.Errorf("open server: %s", err)
	}

	return nil
}

// startServerReporting starts periodic server reporting.
func (s *Server) startServerReporting() {
	s.reportServer()

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.reportServer()
		}
	}
}

// reportServer reports usage statistics about the system.
func (s *Server) reportServer() {
	dbs := s.MetaClient.Databases()
	numDatabases := len(dbs)

	var (
		numMeasurements int64
		numSeries       int64
	)

	for _, db := range dbs {
		name := db.Name
		// Use the context.Background() to avoid timing out on this.
		n, err := s.ClusterStore.SeriesCardinality(context.Background(), name)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Unable to get series cardinality for database %s: %v", name, err))
		} else {
			numSeries += n
		}

		// Use the context.Background() to avoid timing out on this.
		n, err = s.ClusterStore.MeasurementsCardinality(context.Background(), name)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Unable to get measurement cardinality for database %s: %v", name, err))
		} else {
			numMeasurements += n
		}
	}

	serverID := s.MetaClient.NodeID()
	clusterID := s.MetaClient.ClusterID()
	usage := client.Usage{
		Product: "influxdb cluster",
		Data: []client.UsageData{
			{
				Values: client.Values{
					"os":               runtime.GOOS,
					"arch":             runtime.GOARCH,
					"version":          s.buildInfo.Version,
					"node_type":        meta.NodeTypeData,
					"server_id":        fmt.Sprintf("%v", serverID),
					"cluster_id":       fmt.Sprintf("%v", clusterID),
					"num_series":       numSeries,
					"num_measurements": numMeasurements,
					"num_databases":    numDatabases,
					"uptime":           time.Since(startTime).Seconds(),
				},
			},
		},
	}

	fields := []zap.Field{zap.String("product", usage.Product)}
	for k, v := range usage.Data[0].Values {
		fields = append(fields, zap.Any(k, v))
	}
	s.Logger.Info("Reporting usage statistics", fields...)
}

// HTTPAddr returns the HTTP address used by other nodes for HTTP queries and writes.
func (s *Server) HTTPAddr() string {
	return meta.RemoteAddr(s.config.Hostname, s.httpAPIAddr)
}

// HTTPScheme returns the HTTP scheme to specify if we should use a TLS connection.
func (s *Server) HTTPScheme() string {
	if s.httpUseTLS {
		return "https"
	}
	return "http"
}

// TCPAddr returns the TCP address used by other nodes for cluster communication.
func (s *Server) TCPAddr() string {
	return meta.RemoteAddr(s.config.Hostname, s.tcpAddr)
}

// Service represents a service attached to the server.
type Service interface {
	WithLogger(log *zap.Logger)
	Open() error
	Close() error
}

// prof stores the file locations of active profiles.
// StartProfile initializes the cpu and memory profile, if specified.
func (s *Server) startProfile() error {
	if s.CPUProfile != "" {
		f, err := os.Create(s.CPUProfile)
		if err != nil {
			return fmt.Errorf("cpuprofile: %v", err)
		}

		s.CPUProfileWriteCloser = f
		if err := pprof.StartCPUProfile(s.CPUProfileWriteCloser); err != nil {
			return err
		}

		log.Printf("Writing CPU profile to: %s\n", s.CPUProfile)
	}

	if s.MemProfile != "" {
		f, err := os.Create(s.MemProfile)
		if err != nil {
			return fmt.Errorf("memprofile: %v", err)
		}

		s.MemProfileWriteCloser = f
		runtime.MemProfileRate = 4096

		log.Printf("Writing mem profile to: %s\n", s.MemProfile)
	}

	return nil
}

// StopProfile closes the cpu and memory profiles if they are running.
func (s *Server) stopProfile() error {
	if s.CPUProfileWriteCloser != nil {
		pprof.StopCPUProfile()
		if err := s.CPUProfileWriteCloser.Close(); err != nil {
			return err
		}
		log.Println("CPU profile stopped")
	}

	if s.MemProfileWriteCloser != nil {
		pprof.Lookup("heap").WriteTo(s.MemProfileWriteCloser, 0)
		if err := s.MemProfileWriteCloser.Close(); err != nil {
			return err
		}
		log.Println("mem profile stopped")
	}

	return nil
}

func (s *Server) LogQueriesOnTermination() bool {
	if s != nil && s.config != nil {
		return s.config.Coordinator.TerminationQueryLog
	} else {
		return false
	}
}

// monitorPointsWriter is a wrapper around `coordinator.PointsWriter` that helps
// to prevent a circular dependency between the `cluster` and `monitor` packages.
type monitorPointsWriter coordinator.PointsWriter

func (pw *monitorPointsWriter) WritePoints(database, retentionPolicy string, points models.Points) error {
	return (*coordinator.PointsWriter)(pw).WritePointsPrivileged(database, retentionPolicy, models.ConsistencyLevelAny, points)
}
