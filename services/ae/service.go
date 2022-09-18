// Package ae provides the anti-entropy service for InfluxDB.
package ae // import "github.com/influxdata/influxdb/services/ae"

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

// Statistics for the anti-entropy service.
const (
	statBytesRx    = "bytesRx"
	statErrors     = "errors"
	statJobs       = "jobs"
	statJobsActive = "jobsActive"
)

// Service represents the anti-entropy service.
type Service struct {
	MetaClient interface {
		NodeID() uint64
		Databases() []meta.DatabaseInfo
	}
	TSDBStore interface {
		ShardN() int
		Shard(id uint64) *tsdb.Shard
		ShardRelativePath(id uint64) (string, error)
	}

	config Config
	wg     sync.WaitGroup
	done   chan struct{}

	logger *zap.Logger
	stats  *Statistics
}

// NewService returns a configured anti-entropy service.
func NewService(c Config) *Service {
	return &Service{
		config: c,
		logger: zap.NewNop(),
		stats:  &Statistics{},
	}
}

// Open starts anti-entropy.
func (s *Service) Open() error {
	if !s.config.Enabled || s.done != nil {
		return nil
	}

	s.logger.Info("Anti-entropy service is enabled and running")
	s.done = make(chan struct{})

	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.run() }()
	return nil
}

// Close stops anti-entropy.
func (s *Service) Close() error {
	if !s.config.Enabled || s.done == nil {
		return nil
	}

	s.logger.Info("Anti-entropy service closing")
	close(s.done)

	s.wg.Wait()
	s.done = nil
	s.logger.Info("Anti-entropy service closed")

	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.logger = log.With(zap.String("service", "ae"))
}

// Statistics maintains the statistics for the anti-entropy service.
type Statistics struct {
	BytesRx    int64
	Errors     int64
	Jobs       int64
	JobsActive int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "ae",
		Tags: tags,
		Values: map[string]interface{}{
			statBytesRx:    atomic.LoadInt64(&s.stats.BytesRx),
			statErrors:     atomic.LoadInt64(&s.stats.Errors),
			statJobs:       atomic.LoadInt64(&s.stats.Jobs),
			statJobsActive: atomic.LoadInt64(&s.stats.JobsActive),
		},
	}}
}

func (s *Service) run() {
	ticker := time.NewTicker(time.Duration(s.config.CheckInterval))
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return

		case <-ticker.C:
			node := s.MetaClient.NodeID()
			shardsTotal := s.TSDBStore.ShardN()
			shardsChecked := 0
			shardsHot := 0
			shardsMissing := 0
			shardsNoWritesYet := 0
			dbs := s.MetaClient.Databases()
			for _, db := range dbs {
				for _, rp := range db.RetentionPolicies {
					for _, sg := range rp.ShardGroups {
						for _, sh := range sg.Shards {
							if len(sh.Owners) <= 1 {
								continue
							}
							shard := s.TSDBStore.Shard(sh.ID)
							if shard == nil {
								shardsMissing += 1
								continue
							}
							path, err := s.TSDBStore.ShardRelativePath(sh.ID)
							if err != nil {
								shardsMissing += 1
								continue
							}
							engine, err := shard.Engine()
							if err != nil {
								shardsMissing += 1
								continue
							}
							if state, reason := engine.IsIdle(); !state {
								s.logger.Debug("Skipping hot shard",
									zap.Uint64("node", node),
									zap.Uint64("db_shard_id", sh.ID),
									zap.String("reason", fmt.Sprintf("Shard %d at %s %s", sh.ID, path, reason)),
								)
								shardsHot += 1
								continue
							}
						}
					}
				}
			}
			s.logger.Info("Checking status",
				zap.Uint64("node", node),
				zap.Int("shards_total", shardsTotal),
				zap.Int("shards_checked", shardsChecked),
			)
			s.logger.Info("Skipped shards",
				zap.Uint64("node", node),
				zap.Int("hot", shardsHot),
				zap.Int("missing", shardsMissing),
				zap.Int("no_writes_yet", shardsNoWritesYet),
			)
		}
	}
}
