// Package meta provides the meta service for InfluxDB.
package meta // import "github.com/influxdata/influxdb/services/meta"

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

// MuxHeader is the header byte used for the TCP muxer.
const MuxHeader = 8

type Service struct {
	RaftListener net.Listener

	RPCClient RPCClient
	Version   string

	config    *Config
	handler   *handler
	ln        net.Listener
	httpAddr  string
	raftAddr  string
	https     bool
	cert      string
	key       string
	tlsConfig *tls.Config
	err       chan error
	Logger    *zap.Logger
	store     *store
}

// NewService returns a new instance of Service.
func NewService(c *Config) *Service {
	s := &Service{
		config:    c,
		httpAddr:  c.HTTPBindAddress,
		raftAddr:  c.BindAddress,
		Logger:    zap.NewNop(),
		https:     c.HTTPSEnabled,
		cert:      c.HTTPSCertificate,
		key:       c.HTTPSPrivateKey,
		tlsConfig: c.TLS,
		err:       make(chan error),
	}
	if s.tlsConfig == nil {
		s.tlsConfig = new(tls.Config)
	}
	if s.key == "" {
		s.key = s.cert
	}

	return s
}

// Open starts the service
func (s *Service) Open() error {
	s.Logger.Info("Starting meta service")

	if s.config.AuthEnabled && s.config.InternalSharedSecret == "" {
		return errors.New("internal-shared-secret is blank and must be set in config when auth is enabled")
	}

	if s.RaftListener == nil {
		panic("no raft listener set")
	}

	// Open listener.
	if s.https {
		cert, err := tls.LoadX509KeyPair(s.cert, s.key)
		if err != nil {
			return err
		}

		tlsConfig := s.tlsConfig.Clone()
		tlsConfig.Certificates = []tls.Certificate{cert}

		listener, err := tls.Listen("tcp", s.httpAddr, tlsConfig)
		if err != nil {
			return err
		}

		s.ln = listener
	} else {
		listener, err := net.Listen("tcp", s.httpAddr)
		if err != nil {
			return err
		}

		s.ln = listener
	}
	s.Logger.Info("Listening on HTTP",
		zap.Stringer("addr", s.ln.Addr()),
		zap.Bool("https", s.https))

	// wait for the listeners to start
	timeout := time.Now().Add(time.Second)
	for {
		if s.ln.Addr() != nil && s.RaftListener.Addr() != nil {
			break
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("unable to open without http listener running")
		}
		time.Sleep(10 * time.Millisecond)
	}

	var err error
	if autoAssignPort(s.httpAddr) {
		s.httpAddr, err = combineHostAndAssignedPort(s.ln, s.httpAddr)
	}
	if autoAssignPort(s.raftAddr) {
		s.raftAddr, err = combineHostAndAssignedPort(s.RaftListener, s.raftAddr)
	}
	if err != nil {
		return err
	}

	// Open the store.  The addresses passed in are remotely accessible.
	s.store = newStore(s.config, s.HTTPAddr(), s.RaftAddr())
	s.store.WithLogger(s.Logger)

	handler := newHandler(s.config, s)
	handler.WithLogger(s.Logger)
	handler.store = s.store
	s.handler = handler

	// Begin listening for requests in a separate goroutine.
	go s.serve()

	if err := s.store.open(s.RaftListener); err != nil {
		return err
	}

	go s.handler.announce()

	return nil
}

// serve serves the handler from the listener.
func (s *Service) serve() {
	// The listener was closed so exit
	// See https://github.com/golang/go/issues/4373
	err := http.Serve(s.ln, s.handler)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", s.ln.Addr(), err)
	}
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	if err := s.handler.Close(); err != nil {
		return err
	}

	if err := s.store.close(); err != nil {
		return err
	}

	if s.ln != nil {
		if err := s.ln.Close(); err != nil {
			return err
		}
	}

	if s.RaftListener != nil {
		if err := s.RaftListener.Close(); err != nil {
			return err
		}
	}

	return nil
}

// HTTPAddr returns the bind address for the HTTP API
func (s *Service) HTTPAddr() string {
	return RemoteAddr(s.config.RemoteHostname, s.httpAddr)
}

// HTTPScheme returns the HTTP scheme to specify if we should use a TLS connection.
func (s *Service) HTTPScheme() string {
	if s.https {
		return "https"
	}
	return "http"
}

// RaftAddr returns the bind address for the Raft TCP listener
func (s *Service) RaftAddr() string {
	return RemoteAddr(s.config.RemoteHostname, s.raftAddr)
}

func (s *Service) NodeID() uint64 {
	return s.store.nodeID()
}

func (s *Service) ClusterID() uint64 {
	return s.store.clusterID()
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

func autoAssignPort(addr string) bool {
	_, p, _ := net.SplitHostPort(addr)
	return p == "0"
}

func combineHostAndAssignedPort(ln net.Listener, autoAddr string) (string, error) {
	host, _, err := net.SplitHostPort(autoAddr)
	if err != nil {
		return "", err
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, port), nil
}
