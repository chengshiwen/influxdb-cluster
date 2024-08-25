package hh

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// NodeProcessor encapsulates a queue of hinted-handoff data for a node, and the
// transmission of the data to the node.
type NodeProcessor struct {
	PurgeInterval    time.Duration // Interval between periodic purge checks
	RetryInterval    time.Duration // Interval between periodic write-to-node attempts.
	RetryMaxInterval time.Duration // Max interval between periodic write-to-node attempts.
	RetryRateLimit   int64         // Limits the rate data is sent to node.
	MaxWritesPending int           // Maximum number of incoming pending writes.
	MaxSize          int64         // Maximum size an underlying queue can get.
	MaxAge           time.Duration // Maximum age queue data can get before purging.
	nodeID           uint64
	shardID          uint64
	dir              string

	mu   sync.RWMutex
	wg   sync.WaitGroup
	done chan struct{}

	queue  *queue
	meta   metaClient
	writer shardWriter

	stats       *Statistics
	defaultTags models.StatisticTags
	Logger      *zap.Logger
}

// NewNodeProcessor returns a new NodeProcessor for the given node and shard, using dir for
// the hinted-handoff data.
func NewNodeProcessor(cfg Config, nodeID, shardID uint64, dir string, w shardWriter, m metaClient) *NodeProcessor {
	return &NodeProcessor{
		PurgeInterval:    time.Duration(cfg.PurgeInterval),
		RetryInterval:    time.Duration(cfg.RetryInterval),
		RetryMaxInterval: time.Duration(cfg.RetryMaxInterval),
		RetryRateLimit:   cfg.RetryRateLimit,
		MaxWritesPending: cfg.MaxWritesPending,
		MaxSize:          cfg.MaxSize,
		MaxAge:           time.Duration(cfg.MaxAge),
		nodeID:           nodeID,
		shardID:          shardID,
		dir:              dir,
		writer:           w,
		meta:             m,
		stats:            &Statistics{},
		defaultTags:      models.StatisticTags{"node": fmt.Sprintf("%d", nodeID), "shardID": fmt.Sprintf("%d", shardID)},
		Logger:           zap.NewNop(),
	}
}

// WithLogger sets the logger on the node processor.
func (n *NodeProcessor) WithLogger(log *zap.Logger) {
	n.Logger = log
}

// Open opens the NodeProcessor. It will read and write data present in dir, and
// start transmitting data to the node. A NodeProcessor must be opened before it
// can accept hinted data.
func (n *NodeProcessor) Open() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.done != nil {
		// Already open.
		return nil
	}
	n.done = make(chan struct{})

	// Create the queue directory if it doesn't already exist.
	if err := os.MkdirAll(n.dir, 0700); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}

	// Create the queue of hinted-handoff data.
	queue, err := newQueue(n.dir, n.MaxSize, n.MaxWritesPending)
	if err != nil {
		return err
	}
	if err := queue.Open(); err != nil {
		return err
	}
	n.queue = queue

	n.wg.Add(1)
	go n.run()

	return nil
}

// Close closes the NodeProcessor, terminating all data tranmission to the node.
// When closed it will not accept hinted-handoff data.
func (n *NodeProcessor) Close() error {
	if wait := func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()

		if n.closed() {
			return false // Already closed.
		}
		close(n.done)
		return true
	}(); !wait {
		return nil
	}
	n.wg.Wait()

	// Release all remaining resources.
	n.mu.Lock()
	defer n.mu.Unlock()
	n.done = nil
	return n.queue.Close()
}

func (n *NodeProcessor) closed() bool {
	select {
	case <-n.done:
		// NodeProcessor is closing.
		return true
	default:
	}
	return n.done == nil
}

// Statistics returns statistics for periodic monitoring.
func (n *NodeProcessor) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "hh_processor",
		Tags: n.defaultTags.Merge(tags),
		Values: map[string]interface{}{
			statBytesRead:           atomic.LoadInt64(&n.stats.BytesRead),
			statBytesWritten:        atomic.LoadInt64(&n.stats.BytesWritten),
			statQueueBytes:          n.queue.diskUsage(),
			statQueueDepth:          int64(len(n.queue.segments)),
			statWriteBlocked:        atomic.LoadInt64(&n.stats.WriteBlocked),
			statWriteDropped:        atomic.LoadInt64(&n.stats.WriteDropped),
			statWriteShardReq:       atomic.LoadInt64(&n.stats.WriteShardReq),
			statWriteShardReqPoints: atomic.LoadInt64(&n.stats.WriteShardReqPoints),
			statWriteNodeReq:        atomic.LoadInt64(&n.stats.WriteNodeReq),
			statWriteNodeReqFail:    atomic.LoadInt64(&n.stats.WriteNodeReqFail),
			statWriteNodeReqPoints:  atomic.LoadInt64(&n.stats.WriteNodeReqPoints),
		},
	}}
}

// Purge deletes all hinted-handoff data under management by a NodeProcessor.
// The NodeProcessor should be in the closed state before calling this function.
func (n *NodeProcessor) Purge() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.closed() {
		return fmt.Errorf("node processor is open")
	}

	return os.RemoveAll(n.dir)
}

// WriteShard writes hinted-handoff data for the given shard and node. Since it may manipulate
// hinted-handoff queues, and be called concurrently, it takes a lock during queue access.
func (n *NodeProcessor) WriteShard(points []models.Point) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.closed() {
		return fmt.Errorf("node processor is closed")
	}

	atomic.AddInt64(&n.stats.WriteShardReq, 1)
	atomic.AddInt64(&n.stats.WriteShardReqPoints, int64(len(points)))

	i, j := 0, len(points)
	for i < j {
		b := marshalWrite(n.shardID, points[i:j])
		for len(b) > defaultSegmentSize {
			if j == i+1 {
				return ErrSegmentFull
			}
			j = (i + j + 1) / 2
			b = marshalWrite(n.shardID, points[i:j])
		}
		atomic.AddInt64(&n.stats.BytesWritten, int64(len(b)))
		if err := n.queue.Append(b); err != nil {
			switch err {
			case ErrQueueBlocked:
				atomic.AddInt64(&n.stats.WriteBlocked, 1)
			case ErrQueueFull:
				atomic.AddInt64(&n.stats.WriteDropped, 1)
			}
			return err
		}
		if j == len(points) {
			break
		}
		i, j = j, len(points)
	}
	return nil
}

// LastModified returns the time the NodeProcessor last receieved hinted-handoff data.
func (n *NodeProcessor) LastModified() (time.Time, error) {
	t, err := n.queue.LastModified()
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// run attempts to send any existing hinted handoff data to the target node. It also purges
// any hinted handoff data older than the configured time.
func (n *NodeProcessor) run() {
	defer n.wg.Done()

	currInterval := time.Duration(n.RetryInterval)
	if currInterval > time.Duration(n.RetryMaxInterval) {
		currInterval = time.Duration(n.RetryMaxInterval)
	}

	for {
		select {
		case <-n.done:
			return

		case <-time.After(n.PurgeInterval):
			if err := n.queue.PurgeOlderThan(time.Now().Add(-n.MaxAge)); err != nil {
				n.Logger.Error("Failed to purge", zap.Uint64("node", n.nodeID), zap.Uint64("shardID", n.shardID), zap.Error(err))
			}

		case <-time.After(currInterval):
			limiter := NewRateLimiter(n.RetryRateLimit)
			for {
				c, err := n.SendWrite()
				if err != nil {
					if err == io.EOF {
						// No more data, return to configured interval
						currInterval = time.Duration(n.RetryInterval)
					} else {
						currInterval = currInterval * 2
						if currInterval > time.Duration(n.RetryMaxInterval) {
							currInterval = time.Duration(n.RetryMaxInterval)
						}
					}
					break
				}

				// Success! Ensure backoff is cancelled.
				currInterval = time.Duration(n.RetryInterval)

				// Update how many bytes we've sent
				limiter.Update(c)

				// Block to maintain the throughput rate
				time.Sleep(limiter.Delay())
			}
		}
	}
}

// SendWrite attempts to sent the current block of hinted data to the target node. If successful,
// it returns the number of bytes it sent and advances to the next block. Otherwise returns EOF
// when there is no more data or the node is inactive.
func (n *NodeProcessor) SendWrite() (int, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	active, err := n.Active()
	if err != nil {
		return 0, err
	}
	if !active {
		return 0, io.EOF
	}

	// Get the current block from the queue
	buf, err := n.queue.Current()
	if err != nil {
		if err != io.EOF {
			n.Logger.Error("Failed to current queue", zap.Uint64("node", n.nodeID), zap.Uint64("shardID", n.shardID), zap.Error(err))
			// Try to truncate it.
			if err := n.queue.Truncate(); err != nil {
				n.Logger.Error("Failed to truncate queue", zap.Uint64("node", n.nodeID), zap.Uint64("shardID", n.shardID), zap.Error(err))
			}
		} else {
			// Try to skip it.
			if err := n.queue.Advance(); err != nil {
				n.Logger.Error("Failed to advance queue", zap.Uint64("node", n.nodeID), zap.Uint64("shardID", n.shardID), zap.Error(err))
			}
		}
		return 0, err
	}

	// unmarshal the byte slice back to shard ID and points
	_, points, err := unmarshalWrite(buf)
	if err != nil {
		n.Logger.Error("Unmarshal write failed", zap.Uint64("node", n.nodeID), zap.Uint64("shardID", n.shardID), zap.Error(err))
		// Try to skip it.
		if err := n.queue.Advance(); err != nil {
			n.Logger.Error("Failed to advance queue", zap.Uint64("node", n.nodeID), zap.Uint64("shardID", n.shardID), zap.Error(err))
		}
		return 0, err
	}

	if err := n.writer.WriteShardBinary(n.shardID, n.nodeID, points); err != nil && IsRetryable(err) {
		atomic.AddInt64(&n.stats.WriteNodeReqFail, 1)
		return 0, err
	}
	atomic.AddInt64(&n.stats.BytesRead, int64(len(buf)))
	atomic.AddInt64(&n.stats.WriteNodeReq, 1)
	atomic.AddInt64(&n.stats.WriteNodeReqPoints, int64(len(points)))

	if err := n.queue.Advance(); err != nil {
		n.Logger.Error("Failed to advance queue", zap.Uint64("node", n.nodeID), zap.Uint64("shardID", n.shardID), zap.Error(err))
	}

	return len(buf), nil
}

// Head returns the head of the processor's queue.
func (n *NodeProcessor) Head() string {
	qp, err := n.queue.Position()
	if err != nil {
		return ""
	}
	return qp.head
}

// Tail returns the tail of the processor's queue.
func (n *NodeProcessor) Tail() string {
	qp, err := n.queue.Position()
	if err != nil {
		return ""
	}
	return qp.tail
}

// Active returns whether this node processor is for a currently active node.
func (n *NodeProcessor) Active() (bool, error) {
	nio, err := n.meta.DataNode(n.nodeID)
	if err != nil && err != meta.ErrNodeNotFound {
		return false, err
	}
	return nio != nil, nil
}

// Empty returns whether this node processor's queue is empty.
func (n *NodeProcessor) Empty() bool {
	return n.queue.Empty()
}

// IsRetryable returns true if this error is temporary and could be retried
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "field type conflict") || strings.Contains(err.Error(), "partial write") {
		return false
	}
	return true
}

func marshalWrite(shardID uint64, points []models.Point) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, shardID)
	nb := make([]byte, 4)
	for _, p := range points {
		pb, err := p.MarshalBinary()
		if err != nil {
			continue
		}
		binary.BigEndian.PutUint32(nb, uint32(len(pb)))
		b = append(b, nb...)
		b = append(b, pb...)
	}
	return b
}

func unmarshalWrite(b []byte) (uint64, [][]byte, error) {
	if len(b) < 8 {
		return 0, nil, fmt.Errorf("too short: len = %d", len(b))
	}
	shardID, b := binary.BigEndian.Uint64(b[:8]), b[8:]
	var points [][]byte
	var n int
	for len(b) > 0 {
		if len(b) < 4 {
			return shardID, points, io.ErrShortBuffer
		}
		n, b = int(binary.BigEndian.Uint32(b[:4])), b[4:]
		if len(b) < n {
			return shardID, points, io.ErrShortBuffer
		}
		points = append(points, b[:n])
		b = b[n:]
	}
	return shardID, points, nil
}
