package hh

import (
	"fmt"
	"io"
	"os"
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
	queue, err := newQueue(n.dir, n.MaxSize)
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
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.done == nil {
		// Already closed.
		return nil
	}

	close(n.done)
	n.wg.Wait()
	n.done = nil

	return n.queue.Close()
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
			statQueueDepth:          len(n.queue.segments),
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

	if n.done != nil {
		return fmt.Errorf("node processor is open")
	}

	return os.RemoveAll(n.dir)
}

// WriteShard writes hinted-handoff data for the given shard and node. Since it may manipulate
// hinted-handoff queues, and be called concurrently, it takes a lock during queue access.
func (n *NodeProcessor) WriteShard(points []models.Point) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.done == nil {
		return fmt.Errorf("node processor is closed")
	}

	atomic.AddInt64(&n.stats.WriteShardReq, 1)
	atomic.AddInt64(&n.stats.WriteShardReqPoints, int64(len(points)))

	i, j := 0, len(points)
	for i < j {
		b := marshalWrite(points[i:j])
		for len(b) > defaultSegmentSize {
			if j == i+1 {
				return ErrSegmentFull
			}
			j = (i + j + 1) / 2
			b = marshalWrite(points[i:j])
		}
		atomic.AddInt64(&n.stats.BytesWritten, int64(len(b)))
		if err := n.queue.Append(b); err != nil {
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
		return 0, err
	}

	// unmarshal the byte slice back to shard ID and points
	points, err := unmarshalWrite(buf)
	if err != nil {
		atomic.AddInt64(&n.stats.WriteDropped, int64(len(buf)))
		n.Logger.Error("Unmarshal write failed", zap.Error(err))
		// Try to skip it.
		if err := n.queue.Advance(); err != nil {
			n.Logger.Error("Failed to advance queue", zap.Uint64("node", n.nodeID), zap.Uint64("shardID", n.shardID), zap.Error(err))
		}
		return 0, err
	}

	if err := n.writer.WriteShard(n.shardID, n.nodeID, points); err != nil {
		atomic.AddInt64(&n.stats.WriteNodeReqFail, 1)
		return 0, err
	}
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

func marshalWrite(points []models.Point) []byte {
	var b []byte
	if len(points) > 0 {
		b = make([]byte, 0, (len(points[0].String())+1)*len(points))
	}
	for _, p := range points {
		b = append(b, []byte(p.String())...)
		b = append(b, '\n')
	}
	return b
}

func unmarshalWrite(b []byte) ([]models.Point, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("too short: zero")
	}
	return models.ParsePoints(b)
}
