package run

import (
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
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tcp"
	client "github.com/influxdata/usage-client/v1"
	"go.uber.org/zap"
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

	MetaService *meta.Service

	// Server reporting and registration
	reportingDisabled bool

	// Profiling
	CPUProfile            string
	CPUProfileWriteCloser io.WriteCloser
	MemProfile            string
	MemProfileWriteCloser io.WriteCloser

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
	updateTLSConfig(&c.Meta.TLS, tlsConfig)

	// We need to ensure that a meta directory always exists.
	if err := os.MkdirAll(c.Meta.Dir, 0777); err != nil {
		return nil, fmt.Errorf("mkdir all: %s", err)
	}

	// In 0.10.0 bind-address got moved to the top level. Check
	// The old location to keep things backwards compatible
	bind := c.BindAddress
	if c.Meta.BindAddress != "" {
		bind = c.Meta.BindAddress
	}

	s := &Server{
		buildInfo: *buildInfo,
		err:       make(chan error),
		closing:   make(chan struct{}),

		BindAddress: bind,

		Logger: logger.New(os.Stderr),

		MetaService: meta.NewService(c.Meta),

		reportingDisabled: c.ReportingDisabled,

		config: c,
	}

	dataTLSConfig := tcp.TLSClientConfig(c.Meta.DataUseTLS, c.Meta.DataInsecureTLS)
	s.MetaService.RPCClient = coordinator.NewClient(dataTLSConfig, coordinator.DefaultDialTimeout)
	s.MetaService.Version = s.buildInfo.Version
	return s, nil
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
	tlsConfig, err := s.config.Meta.TLSConfig()
	if err != nil {
		return fmt.Errorf("tls config: %s", err)
	}
	ln, err := tcp.ListenTLS("tcp", s.BindAddress, tlsConfig)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln
	s.Logger.Info(fmt.Sprintf("Listening on TCP: %s", ln.Addr().String()))

	// Multiplex listener.
	mux := tcp.NewMux()
	go mux.Serve(ln)

	s.MetaService.RaftListener = mux.Listen(meta.MuxHeader)

	// Configure logging for meta service.
	if s.config.Meta.LoggingEnabled {
		s.MetaService.Logger = s.Logger
	}

	// Open meta service.
	if err := s.MetaService.Open(); err != nil {
		return fmt.Errorf("open meta service: %s", err)
	}

	go s.monitorErrorChan(s.MetaService.Err())

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

	if s.MetaService != nil {
		s.MetaService.Close()
	}

	close(s.closing)
	return nil
}

// monitorErrorChan reads an error channel and resends it through the server.
func (s *Server) monitorErrorChan(ch <-chan error) {
	for {
		select {
		case err, ok := <-ch:
			if !ok {
				return
			}
			s.err <- err
		case <-s.closing:
			return
		}
	}
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
	serverID := s.MetaService.NodeID()
	clusterID := s.MetaService.ClusterID()
	usage := client.Usage{
		Product: "influxdb cluster",
		Data: []client.UsageData{
			{
				Values: client.Values{
					"os":         runtime.GOOS,
					"arch":       runtime.GOARCH,
					"version":    s.buildInfo.Version,
					"node_type":  meta.NodeTypeMeta,
					"server_id":  fmt.Sprintf("%v", serverID),
					"cluster_id": fmt.Sprintf("%v", clusterID),
					"uptime":     time.Since(startTime).Seconds(),
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
