package coordinator

import (
	"bytes"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

func TestWriteShardRequestBinary(t *testing.T) {
	sr := &WriteShardRequest{}

	sr.SetShardID(uint64(1))
	if exp := uint64(1); sr.ShardID() != exp {
		t.Fatalf("ShardID mismatch: got %v, exp %v", sr.ShardID(), exp)
	}

	sr.AddPoint("cpu", 1.0, time.Unix(0, 0), map[string]string{"host": "serverA"})
	sr.AddPoint("cpu", 2.0, time.Unix(0, 0).Add(time.Hour), nil)
	sr.AddPoint("cpu_load", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

	b, err := sr.MarshalBinary()
	if err != nil {
		t.Fatalf("WritePointsRequest.MarshalBinary() failed: %v", err)
	}
	if len(b) == 0 {
		t.Fatalf("WritePointsRequest.MarshalBinary() returned 0 bytes")
	}

	got := &WriteShardRequest{}
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatalf("WritePointsRequest.UnmarshalMarshalBinary() failed: %v", err)
	}

	if got.ShardID() != sr.ShardID() {
		t.Errorf("ShardID mismatch: got %v, exp %v", got.ShardID(), sr.ShardID())
	}

	if len(got.Points()) != len(sr.Points()) {
		t.Errorf("Points count mismatch: got %v, exp %v", len(got.Points()), len(sr.Points()))
	}

	srPoints := sr.Points()
	gotPoints := got.Points()
	for i, p := range srPoints {
		g := gotPoints[i]

		if !bytes.Equal(g.Name(), p.Name()) {
			t.Errorf("Point %d name mismatch: got %v, exp %v", i, g.Name(), p.Name())
		}

		if !g.Time().Equal(p.Time()) {
			t.Errorf("Point %d time mismatch: got %v, exp %v", i, g.Time(), p.Time())
		}

		if g.HashID() != p.HashID() {
			t.Errorf("Point #%d HashID() mismatch: got %v, exp %v", i, g.HashID(), p.HashID())
		}

		for k, v := range p.Tags() {
			gv := g.Tags()[k]
			if !bytes.Equal(gv.Key, v.Key) || !bytes.Equal(gv.Value, v.Value) {
				t.Errorf("Point #%d tag mismatch: got %v, exp %v", i, k, v)
			}
		}

		pFields, _ := p.Fields()
		gFields, _ := g.Fields()
		if len(pFields) != len(gFields) {
			t.Errorf("Point %d field count mismatch: got %v, exp %v", i, len(gFields), len(pFields))
		}

		for j, f := range pFields {
			if gFields[j] != f {
				t.Errorf("Point %d field mismatch: got %v, exp %v", i, gFields[j], f)
			}
		}
	}
}

func TestWriteShardResponseBinary(t *testing.T) {
	sr := &WriteShardResponse{}
	sr.SetCode(10)
	sr.SetMessage("foo")
	b, err := sr.MarshalBinary()

	if exp := 10; sr.Code() != exp {
		t.Fatalf("Code mismatch: got %v, exp %v", sr.Code(), exp)
	}

	if exp := "foo"; sr.Message() != exp {
		t.Fatalf("Message mismatch: got %v, exp %v", sr.Message(), exp)
	}

	if err != nil {
		t.Fatalf("WritePointsResponse.MarshalBinary() failed: %v", err)
	}
	if len(b) == 0 {
		t.Fatalf("WritePointsResponse.MarshalBinary() returned 0 bytes")
	}

	got := &WriteShardResponse{}
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatalf("WritePointsResponse.UnmarshalMarshalBinary() failed: %v", err)
	}

	if got.Code() != sr.Code() {
		t.Errorf("Code mismatch: got %v, exp %v", got.Code(), sr.Code())
	}

	if got.Message() != sr.Message() {
		t.Errorf("Message mismatch: got %v, exp %v", got.Message(), sr.Message())
	}
}

func TestClient_JoinCluster(t *testing.T) {
	dataNode := &meta.NodeInfo{
		ID:      1,
		Addr:    "data1:8086",
		TCPAddr: "data1:8088",
	}
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

		_, err = ReadType(conn)
		if err != nil {
			if strings.HasSuffix(err.Error(), "EOF") {
				return
			}
			t.Errorf("Unable to read type: %s", err)
			return
		}

		var req JoinClusterRequest
		if err = DecodeLV(conn, &req); err != nil {
			t.Errorf("Unable to decode length-value: %s", err)
		}

		if !reflect.DeepEqual(req.MetaServers, metaServers) {
			t.Errorf("Unexpected meta servers: %v", req.MetaServers)
		}

		if err = EncodeTLV(conn, joinClusterResponseMessage, &JoinClusterResponse{Node: dataNode}); err != nil {
			t.Errorf("Unable to write JoinCluster response: %s", err)
		}
	}()

	c := NewClient(nil, DefaultDialTimeout)
	node, err := c.JoinCluster(l.Addr().String(), metaServers, true)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if !reflect.DeepEqual(node, dataNode) {
		t.Errorf("unexpected node: %v", node)
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

		_, err = ReadType(conn)
		if err != nil {
			if strings.HasSuffix(err.Error(), "EOF") {
				return
			}
			t.Errorf("Unable to read type: %s", err)
			return
		}

		if err = EncodeTLV(conn, leaveClusterResponseMessage, &LeaveClusterResponse{}); err != nil {
			t.Errorf("Unable to write LeaveCluster response: %s", err)
		}
	}()

	c := NewClient(nil, DefaultDialTimeout)
	err = c.LeaveCluster(l.Addr().String())
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
