package ae_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/ae"
)

func TestService_OpenDisabled(t *testing.T) {
	// Opening a disabled service should be a no-op.
	c := ae.NewConfig()
	s := NewService(c)

	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	if s.LogBuf.String() != "" {
		t.Fatalf("service logged %q, didn't expect any logging", s.LogBuf.String())
	}
}

func TestService_OpenClose(t *testing.T) {
	// Opening a disabled service should be a no-op.
	c := ae.NewConfig()
	c.Enabled = true
	s := NewService(c)

	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	if s.LogBuf.String() == "" {
		t.Fatal("service didn't log anything on open")
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
	TSDBStore  *internal.TSDBStoreMock

	LogBuf bytes.Buffer
	*ae.Service
}

func NewService(c ae.Config) *Service {
	s := &Service{
		MetaClient: &internal.MetaClientMock{},
		TSDBStore:  &internal.TSDBStoreMock{},
		Service:    ae.NewService(c),
	}

	l := logger.New(&s.LogBuf)
	s.WithLogger(l)

	s.Service.MetaClient = s.MetaClient
	s.Service.TSDBStore = s.TSDBStore
	return s
}
