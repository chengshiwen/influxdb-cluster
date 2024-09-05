package coordinator_test

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/models"
)

// Ensure the shard writer can successfully write a single request.
func TestShardWriter_WriteShard_Success(t *testing.T) {
	ts := newTestWriteService(nil)
	ts.TSDBStore.WriteToShardFn = ts.writeShardSuccess
	s := coordinator.NewService(coordinator.Config{})
	s.Listener = ts.muxln
	s.DefaultListener = ts.defln
	s.MetaClient = &metaClient{addr: ts.ln.Addr().String()}
	s.TSDBStore = &ts.TSDBStore
	s.Server = &server{}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer ts.Close()

	w := coordinator.NewShardWriter(10*time.Second, time.Second, time.Minute, 1)
	w.MetaClient = &metaClient{addr: ts.ln.Addr().String()}

	// Build a single point.
	now := time.Now()
	var points []models.Point
	points = append(points, models.MustNewPoint("cpu", models.NewTags(map[string]string{"host": "server01"}), map[string]interface{}{"value": int64(100)}, now))

	// Write to shard and close.
	if err := w.WriteShard(1, 2, points); err != nil {
		t.Fatal(err)
	} else if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Validate response.
	responses, err := ts.ResponseN(1)
	if err != nil {
		t.Fatal(err)
	} else if responses[0].shardID != 1 {
		t.Fatalf("unexpected shard id: %d", responses[0].shardID)
	}

	// Validate point.
	if p := responses[0].points[0]; string(p.Name()) != "cpu" {
		t.Fatalf("unexpected name: %s", p.Name())
	} else if fields, _ := p.Fields(); fields["value"] != int64(100) {
		t.Fatalf("unexpected 'value' field: %d", fields["value"])
	} else if string(p.Tags().Get([]byte("host"))) != "server01" {
		t.Fatalf("unexpected 'host' tag: %s", p.Tags().Get([]byte("host")))
	} else if p.Time().UnixNano() != now.UnixNano() {
		t.Fatalf("unexpected time: %s", p.Time())
	}
}

// Ensure the shard writer can successful write a multiple requests.
func TestShardWriter_WriteShard_Multiple(t *testing.T) {
	ts := newTestWriteService(nil)
	ts.TSDBStore.WriteToShardFn = ts.writeShardSuccess
	s := coordinator.NewService(coordinator.Config{})
	s.Listener = ts.muxln
	s.DefaultListener = ts.defln
	s.MetaClient = &metaClient{addr: ts.ln.Addr().String()}
	s.TSDBStore = &ts.TSDBStore
	s.Server = &server{}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer ts.Close()

	w := coordinator.NewShardWriter(10*time.Second, time.Second, time.Minute, 1)
	w.MetaClient = &metaClient{addr: ts.ln.Addr().String()}

	// Build a single point.
	now := time.Now()
	var points []models.Point
	points = append(points, models.MustNewPoint("cpu", models.NewTags(map[string]string{"host": "server01"}), map[string]interface{}{"value": int64(100)}, now))

	// Write to shard twice and close.
	if err := w.WriteShard(1, 2, points); err != nil {
		t.Fatal(err)
	} else if err := w.WriteShard(1, 2, points); err != nil {
		t.Fatal(err)
	} else if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Validate response.
	responses, err := ts.ResponseN(1)
	if err != nil {
		t.Fatal(err)
	} else if responses[0].shardID != 1 {
		t.Fatalf("unexpected shard id: %d", responses[0].shardID)
	}

	// Validate point.
	if p := responses[0].points[0]; string(p.Name()) != "cpu" {
		t.Fatalf("unexpected name: %s", p.Name())
	} else if fields, _ := p.Fields(); fields["value"] != int64(100) {
		t.Fatalf("unexpected 'value' field: %d", fields["value"])
	} else if string(p.Tags().Get([]byte("host"))) != "server01" {
		t.Fatalf("unexpected 'host' tag: %s", p.Tags().Get([]byte("host")))
	} else if p.Time().UnixNano() != now.UnixNano() {
		t.Fatalf("unexpected time: %s", p.Time())
	}
}

// Ensure the shard writer returns an error when the server fails to accept the write.
func TestShardWriter_WriteShard_Error(t *testing.T) {
	ts := newTestWriteService(writeShardFail)
	s := coordinator.NewService(coordinator.Config{})
	s.Listener = ts.muxln
	s.DefaultListener = ts.defln
	s.MetaClient = &metaClient{addr: ts.ln.Addr().String()}
	s.TSDBStore = &ts.TSDBStore
	s.Server = &server{}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer ts.Close()

	w := coordinator.NewShardWriter(10*time.Second, time.Second, time.Minute, 1)
	w.MetaClient = &metaClient{addr: ts.ln.Addr().String()}
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []models.Point
	points = append(points, models.MustNewPoint(
		"cpu", models.NewTags(map[string]string{"host": "server01"}), map[string]interface{}{"value": int64(100)}, now,
	))

	if err := w.WriteShard(shardID, ownerID, points); err == nil || err.Error() != "error code 1: write shard 1: failed to write" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Ensure the shard writer returns an error when reading times out.
func TestShardWriter_Write_ErrReadTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	w := coordinator.NewShardWriter(10*time.Second, time.Second, time.Minute, 1)
	w.MetaClient = &metaClient{addr: ln.Addr().String()}
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []models.Point
	points = append(points, models.MustNewPoint(
		"cpu", models.NewTags(map[string]string{"host": "server01"}), map[string]interface{}{"value": int64(100)}, now,
	))

	if err := w.WriteShard(shardID, ownerID, points); err == nil || !strings.Contains(err.Error(), "i/o timeout") {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the shard writer returns an error when we can't get a connection.
func TestShardWriter_Write_PoolMax(t *testing.T) {
	ts := newTestWriteService(writeShardSlow)
	s := coordinator.NewService(coordinator.Config{})
	s.Listener = ts.muxln
	s.DefaultListener = ts.defln
	s.MetaClient = &metaClient{addr: ts.ln.Addr().String()}
	s.TSDBStore = &ts.TSDBStore
	s.Server = &server{}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer ts.Close()

	w := coordinator.NewShardWriter(10*time.Second, time.Second, 100*time.Minute, 0)
	w.MetaClient = &metaClient{addr: ts.ln.Addr().String()}
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []models.Point
	points = append(points, models.MustNewPoint(
		"cpu", models.NewTags(map[string]string{"host": "server01"}), map[string]interface{}{"value": int64(100)}, now,
	))

	go w.WriteShard(shardID, ownerID, points)
	time.Sleep(time.Millisecond)
	if err := w.WriteShard(shardID, ownerID, points); err == nil || err.Error() != "invalid capacity settings" {
		t.Fatalf("unexpected error: %v", err)
	}
}
