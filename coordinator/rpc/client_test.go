package rpc_test

import (
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/coordinator/rpc"
)

func TestClient_JoinCluster(t *testing.T) {
	metaServers := []string{"meta1:8089", "meta2:8089"}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer l.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			t.Errorf("error accepting tcp connection: %s", err)
			return
		}
		defer conn.Close()

		var header [1]byte
		if _, err = conn.Read(header[:]); err != nil {
			t.Errorf("unable to read mux header: %s", err)
			return
		}

		_, err = rpc.ReadType(conn)
		if err != nil {
			if strings.HasSuffix(err.Error(), "EOF") {
				return
			}
			t.Errorf("Unable to read type: %s", err)
			return
		}

		var req rpc.JoinClusterRequest
		if err = rpc.DecodeLV(conn, &req); err != nil {
			t.Errorf("Unable to decode length-value: %s", err)
		}

		if !reflect.DeepEqual(req.MetaServers, metaServers) {
			t.Errorf("Unexpected meta servers: %v", req.MetaServers)
		}

		if err = rpc.EncodeTLV(conn, rpc.JoinClusterResponseMessage, &rpc.JoinClusterResponse{}); err != nil {
			t.Errorf("Unable to write JoinCluster response: %s", err)
		}
	}()

	c := rpc.NewClient(l.Addr().String())
	err = c.JoinCluster(metaServers, true)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		t.Errorf("timeout while waiting for the goroutine")
	}
}

func TestClient_LeaveCluster(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer l.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			t.Errorf("error accepting tcp connection: %s", err)
			return
		}
		defer conn.Close()

		var header [1]byte
		if _, err = conn.Read(header[:]); err != nil {
			t.Errorf("unable to read mux header: %s", err)
			return
		}

		_, err = rpc.ReadType(conn)
		if err != nil {
			if strings.HasSuffix(err.Error(), "EOF") {
				return
			}
			t.Errorf("Unable to read type: %s", err)
			return
		}

		if err = rpc.EncodeTLV(conn, rpc.LeaveClusterResponseMessage, &rpc.LeaveClusterResponse{}); err != nil {
			t.Errorf("Unable to write LeaveCluster response: %s", err)
		}
	}()

	c := rpc.NewClient(l.Addr().String())
	err = c.LeaveCluster()
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		t.Errorf("timeout while waiting for the goroutine")
	}
}
