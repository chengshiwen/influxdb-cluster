// Package hh provides the hinted handoff service for InfluxDB.
package hh // import "github.com/influxdata/influxdb/services/hh"

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// ErrHintedHandoffDisabled is returned when attempting to use a
// disabled hinted handoff service.
var ErrHintedHandoffDisabled = fmt.Errorf("hinted handoff disabled")

const (
	statBytesRead           = "bytesRead"
	statBytesWritten        = "bytesWritten"
	statQueueBytes          = "queueBytes"
	statQueueDepth          = "queueDepth"
	statWriteBlocked        = "writeBlocked"
	statWriteDropped        = "writeDropped"
	statWriteShardReq       = "writeShardReq"
	statWriteShardReqPoints = "writeShardReqPoints"
	statWriteNodeReq        = "writeNodeReq"
	statWriteNodeReqFail    = "writeNodeReqFail"
	statWriteNodeReqPoints  = "writeNodeReqPoints"
)

// Service represents a hinted handoff service.
type Service struct {
	mu      sync.RWMutex
	wg      sync.WaitGroup
	closing chan struct{}

	processors map[uint64]*NodeProcessor

	stats       *Statistics
	defaultTags models.StatisticTags
	Logger      *zap.Logger
	cfg         Config

	shardWriter shardWriter
	MetaClient  metaClient

	Monitor interface {
		RegisterDiagnosticsClient(name string, client diagnostics.Client)
		DeregisterDiagnosticsClient(name string)
	}
}

type shardWriter interface {
	WriteShard(shardID, ownerID uint64, points []models.Point) error
}

type metaClient interface {
	DataNode(id uint64) (ni *meta.NodeInfo, err error)
}

// NewService returns a new instance of Service.
func NewService(c Config, w shardWriter) *Service {
	return &Service{
		cfg:         c,
		closing:     make(chan struct{}),
		processors:  make(map[uint64]*NodeProcessor),
		stats:       &Statistics{},
		defaultTags: models.StatisticTags{"path": c.Dir},
		Logger:      zap.NewNop(),
		shardWriter: w,
	}
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "handoff"))
}

// Statistics maintains the statistics for the hinted handoff service.
type Statistics struct {
	BytesRead           int64
	BytesWritten        int64
	QueueBytes          int64
	QueueDepth          int64
	WriteBlocked        int64
	WriteDropped        int64
	WriteShardReq       int64
	WriteShardReqPoints int64
	WriteNodeReq        int64
	WriteNodeReqFail    int64
	WriteNodeReqPoints  int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	statistics := []models.Statistic{{
		Name: "hh",
		Tags: s.defaultTags.Merge(tags),
		Values: map[string]interface{}{
			statBytesRead:           atomic.LoadInt64(&s.stats.BytesRead),
			statBytesWritten:        atomic.LoadInt64(&s.stats.BytesWritten),
			statQueueBytes:          atomic.LoadInt64(&s.stats.QueueBytes),
			statQueueDepth:          atomic.LoadInt64(&s.stats.QueueDepth),
			statWriteBlocked:        atomic.LoadInt64(&s.stats.WriteBlocked),
			statWriteDropped:        atomic.LoadInt64(&s.stats.WriteDropped),
			statWriteShardReq:       atomic.LoadInt64(&s.stats.WriteShardReq),
			statWriteShardReqPoints: atomic.LoadInt64(&s.stats.WriteShardReqPoints),
			statWriteNodeReq:        atomic.LoadInt64(&s.stats.WriteNodeReq),
			statWriteNodeReqFail:    atomic.LoadInt64(&s.stats.WriteNodeReqFail),
			statWriteNodeReqPoints:  atomic.LoadInt64(&s.stats.WriteNodeReqPoints),
		},
	}}
	for _, p := range s.processors {
		statistics = append(statistics, p.Statistics(nil)...)
	}
	return statistics
}

// Open opens the hinted handoff service.
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.cfg.Enabled {
		// Allow Open to proceed, but don't do anything.
		return nil
	}
	s.Logger.Info("Starting hinted handoff service")
	s.closing = make(chan struct{})

	// Register diagnostics if a Monitor service is available.
	if s.Monitor != nil {
		s.Monitor.RegisterDiagnosticsClient("hh", s)
	}

	// Create the root directory if it doesn't already exist.
	s.Logger.Info("Using data dir", zap.String("path", s.cfg.Dir))
	if err := os.MkdirAll(s.cfg.Dir, 0700); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}

	// Create a node processor for each node directory.
	files, err := ioutil.ReadDir(s.cfg.Dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		nodeID, err := strconv.ParseUint(file.Name(), 10, 64)
		if err != nil {
			// Not a number? Skip it.
			continue
		}

		n := NewNodeProcessor(s.cfg, nodeID, s.pathforNode(nodeID), s.shardWriter, s.MetaClient, s.Logger)
		if err := n.Open(); err != nil {
			return err
		}
		s.processors[nodeID] = n
	}

	s.wg.Add(1)
	go s.purgeInactiveProcessors()

	return nil
}

// Close closes the hinted handoff service.
func (s *Service) Close() error {
	s.Logger.Info("Shutting down hinted handoff service")
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, p := range s.processors {
		if err := p.Close(); err != nil {
			return err
		}
	}

	if s.Monitor != nil {
		s.Monitor.DeregisterDiagnosticsClient("hh")
	}

	if s.closing != nil {
		close(s.closing)
	}
	s.wg.Wait()
	s.closing = nil

	return nil
}

// WriteShard queues the points write for shardID to node ownerID to handoff queue
func (s *Service) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	if !s.cfg.Enabled {
		return ErrHintedHandoffDisabled
	}
	atomic.AddInt64(&s.stats.WriteShardReq, 1)
	atomic.AddInt64(&s.stats.WriteShardReqPoints, int64(len(points)))

	s.mu.RLock()
	processor, ok := s.processors[ownerID]
	s.mu.RUnlock()
	if !ok {
		if err := func() error {
			// Check again under write-lock.
			s.mu.Lock()
			defer s.mu.Unlock()

			processor, ok = s.processors[ownerID]
			if !ok {
				processor = NewNodeProcessor(s.cfg, ownerID, s.pathforNode(ownerID), s.shardWriter, s.MetaClient, s.Logger)
				if err := processor.Open(); err != nil {
					return err
				}
				s.processors[ownerID] = processor
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	if err := processor.WriteShard(shardID, points); err != nil {
		return err
	}

	return nil
}

// Diagnostics returns diagnostic information.
func (s *Service) Diagnostics() (*diagnostics.Diagnostics, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	d := &diagnostics.Diagnostics{
		Columns: []string{"node", "active", "last modified", "head", "tail"},
		Rows:    make([][]interface{}, 0, len(s.processors)),
	}

	for k, v := range s.processors {
		lm, err := v.LastModified()
		if err != nil {
			return nil, err
		}

		active, err := v.Active()
		if err != nil {
			return nil, err
		}

		d.Rows = append(d.Rows, []interface{}{k, active, lm, v.Head(), v.Tail()})
	}
	return d, nil
}

// purgeInactiveProcessors will cause the service to remove processors for inactive nodes.
func (s *Service) purgeInactiveProcessors() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Duration(s.cfg.PurgeInterval))
	defer ticker.Stop()

	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()

				for k, v := range s.processors {
					lm, err := v.LastModified()
					if err != nil {
						s.Logger.Info("Failed to determine LastModified for processor", zap.Uint64("node", k), zap.Error(err))
						continue
					}

					active, err := v.Active()
					if err != nil {
						s.Logger.Info("Failed to determine if node is active", zap.Uint64("node", k), zap.Error(err))
						continue
					}
					if active {
						// Node is active.
						continue
					}

					if !lm.Before(time.Now().Add(-time.Duration(s.cfg.MaxAge))) {
						// Node processor contains too-young data.
						continue
					}

					if err := v.Close(); err != nil {
						s.Logger.Info("Failed to close node processor", zap.Uint64("node", k), zap.Error(err))
						continue
					}
					if err := v.Purge(); err != nil {
						s.Logger.Info("Failed to purge node processor", zap.Uint64("node", k), zap.Error(err))
						continue
					}
					delete(s.processors, k)
				}
			}()
		}
	}
}

// pathforNode returns the directory for HH data, for the given node.
func (s *Service) pathforNode(nodeID uint64) string {
	return filepath.Join(s.cfg.Dir, fmt.Sprintf("%d", nodeID))
}
