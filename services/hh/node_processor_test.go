package hh

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
)

type fakeShardWriter struct {
	ShardWriteFn func(shardID, nodeID uint64, points [][]byte) error
}

func (f *fakeShardWriter) WriteShardBinary(shardID, nodeID uint64, points [][]byte) error {
	return f.ShardWriteFn(shardID, nodeID, points)
}

type fakeMetaStore struct {
	NodeFn func(nodeID uint64) (*meta.NodeInfo, error)
}

func (f *fakeMetaStore) DataNode(nodeID uint64) (*meta.NodeInfo, error) {
	return f.NodeFn(nodeID)
}

func TestNodeProcessorSendBlock(t *testing.T) {
	dir, err := os.MkdirTemp("", "node_processor_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// expected data to be queue and sent to the shardWriter
	var expShardID, expNodeID, count = uint64(100), uint64(200), 0
	pt := models.MustNewPoint("cpu", models.NewTags(map[string]string{"foo": "bar"}), models.Fields{"value": 1.0}, time.Unix(0, 0))

	sh := &fakeShardWriter{
		ShardWriteFn: func(shardID, nodeID uint64, points [][]byte) error {
			count++
			if shardID != expShardID {
				t.Errorf("SendWrite() shardID mismatch: got %v, exp %v", shardID, expShardID)
			}
			if nodeID != expNodeID {
				t.Errorf("SendWrite() nodeID mismatch: got %v, exp %v", nodeID, expNodeID)
			}

			if exp := 1; len(points) != exp {
				t.Fatalf("SendWrite() points mismatch: got %v, exp %v", len(points), exp)
			}

			p, err := models.NewPointFromBytes(points[0])
			if err != nil {
				t.Fatalf("SendWrite() point bytes mismatch: got %v, exp %v", err, pt.String())
			}

			if p.String() != pt.String() {
				t.Fatalf("SendWrite() points mismatch:\n got %v\n exp %v", p.String(), pt.String())
			}

			return nil
		},
	}
	metastore := &fakeMetaStore{
		NodeFn: func(nodeID uint64) (*meta.NodeInfo, error) {
			if nodeID == expNodeID {
				return &meta.NodeInfo{}, nil
			}
			return nil, nil
		},
	}

	n := NewNodeProcessor(NewConfig(), expNodeID, expShardID, dir, sh, metastore)
	if n == nil {
		t.Fatalf("Failed to create node processor: %v", err)
	}

	if err := n.Open(); err != nil {
		t.Fatalf("Failed to open node processor: %v", err)
	}

	// Check the active state.
	active, err := n.Active()
	if err != nil {
		t.Fatalf("Failed to check node processor state: %v", err)
	}
	if !active {
		t.Fatalf("Node processor state is unexpected value of: %v", active)
	}

	// This should queue a write for the active node.
	if err := n.WriteShard([]models.Point{pt}); err != nil {
		t.Fatalf("SendWrite() failed to write points: %v", err)
	}

	// This should send the write to the shard writer
	if _, err := n.SendWrite(); err != nil {
		t.Fatalf("SendWrite() failed to write points: %v", err)
	}

	if exp := 1; count != exp {
		t.Fatalf("SendWrite() write count mismatch: got %v, exp %v", count, exp)
	}

	// All data should have been handled so no writes should be sent again
	if _, err := n.SendWrite(); err != nil && err != io.EOF {
		t.Fatalf("SendWrite() failed to write points: %v", err)
	}

	// Count should stay the same
	if exp := 1; count != exp {
		t.Fatalf("SendWrite() write count mismatch: got %v, exp %v", count, exp)
	}

	// Make the node inactive.
	sh.ShardWriteFn = func(shardID, nodeID uint64, points [][]byte) error {
		t.Fatalf("write sent to inactive node")
		return nil
	}
	metastore.NodeFn = func(nodeID uint64) (*meta.NodeInfo, error) {
		return nil, nil
	}

	// Check the active state.
	active, err = n.Active()
	if err != nil {
		t.Fatalf("Failed to check node processor state: %v", err)
	}
	if active {
		t.Fatalf("Node processor state is unexpected value of: %v", active)
	}

	// This should queue a write for the node.
	if err := n.WriteShard([]models.Point{pt}); err != nil {
		t.Fatalf("SendWrite() failed to write points: %v", err)
	}

	// This should not send the write to the shard writer since the node is inactive.
	if _, err := n.SendWrite(); err != nil && err != io.EOF {
		t.Fatalf("SendWrite() failed to write points: %v", err)
	}

	if exp := 1; count != exp {
		t.Fatalf("SendWrite() write count mismatch: got %v, exp %v", count, exp)
	}

	if err := n.Close(); err != nil {
		t.Fatalf("Failed to close node processor: %v", err)
	}

	// Confirm that purging works ok.
	if err := n.Purge(); err != nil {
		t.Fatalf("Failed to purge node processor: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("Node processor directory still present after purge")
	}
}

func TestNodeProcessorMarshalWrite(t *testing.T) {
	expShardID := uint64(127)
	expPointsStr := `cpu value1=1.0,value2=1.0,value3=3.0,value4=4,value5="five" 1000000000
cpu,env=prod,host=serverA,region=us-west,tag1=value1,tag2=value2,tag3=value3,tag4=value4,tag5=value5,target=servers,zone=1c value=1i 1000000000`
	points, _ := models.ParsePointsString(expPointsStr)
	b := marshalWrite(expShardID, points)

	shardID, pts, err := unmarshalWrite(b)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if shardID != expShardID {
		t.Fatalf("unexpected shardID: %d, exp: %d", shardID, expShardID)
	}

	var lines []string
	for _, pt := range pts {
		p, err := models.NewPointFromBytes(pt)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		lines = append(lines, p.String())
	}
	pointsStr := strings.Join(lines, "\n")
	if pointsStr != expPointsStr {
		t.Fatalf("unexpected points string: %s, exp: %s", pointsStr, expPointsStr)
	}
}
