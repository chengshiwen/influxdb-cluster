package announcer_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/announcer"
	"github.com/influxdata/influxdb/services/meta"
)

type server struct{}

func (s *server) HTTPAddr() string {
	return "127.0.0.1:8086"
}

func (s *server) HTTPScheme() string {
	return "http"
}

func (s *server) TCPAddr() string {
	return "127.0.0.1:8088"
}

func TestService_OpenClose(t *testing.T) {
	// Opening a disabled service should be a no-op.
	s := NewService(meta.NewConfig())

	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	if s.LogBuf.String() != "" {
		t.Fatal("service shouldn't log anything on open")
	}

	// Reopening is a no-op
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Re-closing is a no-op
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

type Service struct {
	MetaClient *internal.MetaClientMock

	LogBuf bytes.Buffer
	*announcer.Service
}

func NewService(c *meta.Config) *Service {
	s := &Service{
		MetaClient: &internal.MetaClientMock{},
		Service:    announcer.NewService(c),
	}

	l := logger.New(&s.LogBuf)
	s.WithLogger(l)

	s.Service.Server = &server{}
	s.Service.MetaClient = s.MetaClient
	return s
}
