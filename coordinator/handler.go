package coordinator

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"sync"

	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// handler is an http.Handler for the coordinator service.
type handler struct {
	s        *Service
	hostname string
	tcpBind  string
	tcpAddr  string
	httpAddr string
	logger   *zap.Logger
}

// newHandler returns a new instance of handler with routes.
func newHandler(s *Service) *handler {
	host, port, _ := net.SplitHostPort(s.Server.TCPAddr())
	h := &handler{
		s:        s,
		hostname: host,
		tcpBind:  ":" + port,
		tcpAddr:  s.Server.TCPAddr(),
		httpAddr: s.Server.HTTPAddr(),
		logger:   s.Logger,
	}

	return h
}

// ServeHTTP responds to HTTP request to the handler.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/status":
		h.serveStatus(w, r)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (h *handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	nodeID := h.s.MetaClient.NodeID()
	metaServers := h.s.MetaClient.MetaServers()
	nodeStatus := meta.NodeStatusDisjoined
	if nodeID > 0 && len(metaServers) > 0 {
		nodeStatus = meta.NodeStatusJoined
	}
	ns := &meta.DataNodeStatus{
		NodeType:   meta.NodeTypeData,
		Hostname:   h.hostname,
		TCPBind:    h.tcpBind,
		TCPAddr:    h.tcpAddr,
		HTTPAddr:   h.httpAddr,
		NodeStatus: nodeStatus,
		MetaAddrs:  metaServers,
	}

	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(ns); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
	}
}

// httpError writes an error to the client in a standard format.
func (h *handler) httpError(w http.ResponseWriter, errmsg string, code int) {
	if code/100 != 2 {
		sz := math.Min(float64(len(errmsg)), 1024.0)
		w.Header().Set("X-InfluxDB-Error", errmsg[:int(sz)])
	}

	er := &meta.ErrorResponse{Err: errmsg}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(code)
	b, _ := json.Marshal(er)
	w.Write(b)
}

// chanListener represents a listener that receives connections through a channel.
type chanListener struct {
	addr   net.Addr
	ch     chan net.Conn
	done   chan struct{}
	closer sync.Once // closer ensures that Close is idempotent.
}

// newChanListener returns a new instance of chanListener.
func newChanListener(addr net.Addr) *chanListener {
	return &chanListener{
		addr: addr,
		ch:   make(chan net.Conn),
		done: make(chan struct{}),
	}
}

func (ln *chanListener) Accept() (net.Conn, error) {
	errClosed := errors.New("network connection closed")
	select {
	case <-ln.done:
		return nil, errClosed
	case conn, ok := <-ln.ch:
		if !ok {
			return nil, errClosed
		}
		return conn, nil
	}
}

// Close closes the connection channel.
func (ln *chanListener) Close() error {
	ln.closer.Do(func() {
		close(ln.done)
	})
	return nil
}

// Addr returns the network address of the listener.
func (ln *chanListener) Addr() net.Addr { return ln.addr }

// readerConn represents a net.Conn with an assignable reader.
type readerConn struct {
	net.Conn
	r io.Reader
}

// Read implements the io.Reader interface.
func (conn *readerConn) Read(b []byte) (n int, err error) { return conn.r.Read(b) }
