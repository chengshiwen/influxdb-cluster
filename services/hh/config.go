package hh

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultMaxSize is the default maximum size of all hinted handoff queues in bytes.
	DefaultMaxSize = 10 * 1024 * 1024 * 1024

	// DefaultMaxAge is the default maximum amount of time that a hinted handoff write
	// can stay in the queue.  After this time, the write will be purged.
	DefaultMaxAge = 7 * 24 * time.Hour

	// DefaultRetryConcurrency is the maximum number of hinted handoff blocks that the source
	// data node attempts to write to each destination data node. Hinted handoff blocks are
	// sets of data that belong to the same shard and have the same destination data node.
	DefaultRetryConcurrency = 20

	// DefaultRetryRateLimit is the default rate that hinted handoffs will be retried.
	// The rate is in bytes per second and applies across all nodes when retried. A
	// value of 0 disables the rate limit.
	DefaultRetryRateLimit = 0

	// DefaultRetryInterval is the default amount of time the system waits before
	// attempting to flush hinted handoff queues. With each failure of a hinted
	// handoff write, this retry interval increases exponentially until it reaches
	// the maximum
	DefaultRetryInterval = time.Second

	// DefaultRetryMaxInterval is the maximum the hinted handoff retry interval
	// will ever be.
	DefaultRetryMaxInterval = 10 * time.Second

	// DefaultPurgeInterval is the amount of time the system waits before attempting
	// to purge hinted handoff data due to age or inactive nodes.
	DefaultPurgeInterval = time.Minute

	// DefaultBatchSize is the maximum number of bytes to write to a shard in a single request.
	DefaultBatchSize = 512000

	// DefaultMaxWritesPending is the maximum number of incoming pending writes
	// allowed in the hinted handoff queue.
	DefaultMaxWritesPending = 1024
)

// Config is a hinted handoff configuration.
type Config struct {
	Enabled          bool          `toml:"enabled"`
	Dir              string        `toml:"dir"`
	MaxSize          int64         `toml:"max-size"`
	MaxAge           toml.Duration `toml:"max-age"`
	RetryConcurrency int           `toml:"retry-concurrency"`
	RetryRateLimit   int64         `toml:"retry-rate-limit"`
	RetryInterval    toml.Duration `toml:"retry-interval"`
	RetryMaxInterval toml.Duration `toml:"retry-max-interval"`
	PurgeInterval    toml.Duration `toml:"purge-interval"`
	BatchSize        int64         `toml:"batch-size"`
	MaxWritesPending int           `toml:"max-writes-pending"`
}

// NewConfig returns a new Config.
func NewConfig() Config {
	return Config{
		Enabled:          true,
		MaxSize:          DefaultMaxSize,
		MaxAge:           toml.Duration(DefaultMaxAge),
		RetryConcurrency: DefaultRetryConcurrency,
		RetryRateLimit:   DefaultRetryRateLimit,
		RetryInterval:    toml.Duration(DefaultRetryInterval),
		RetryMaxInterval: toml.Duration(DefaultRetryMaxInterval),
		PurgeInterval:    toml.Duration(DefaultPurgeInterval),
		BatchSize:        DefaultBatchSize,
		MaxWritesPending: DefaultMaxWritesPending,
	}
}

func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.Dir == "" {
		return errors.New("HintedHandoff.Dir must be specified")
	}
	if c.MaxAge <= 0 {
		return errors.New("max-age must be positive")
	}
	if c.RetryInterval <= 0 {
		return errors.New("retry-interval be positive")
	}
	if c.RetryMaxInterval <= 0 {
		return errors.New("retry-max-interval must be positive")
	}
	if c.PurgeInterval <= 0 {
		return errors.New("purge-interval must be positive")
	}
	if c.MaxWritesPending < 0 {
		return errors.New("max-writes-pending must be non-negative")
	}

	return nil
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	if !c.Enabled {
		return diagnostics.RowFromMap(map[string]interface{}{
			"enabled": false,
		}), nil
	}

	return diagnostics.RowFromMap(map[string]interface{}{
		"enabled":            true,
		"dir":                c.Dir,
		"max-size":           c.MaxSize,
		"max-age":            c.MaxAge,
		"retry-concurrency":  c.RetryConcurrency,
		"retry-rate-limit":   c.RetryRateLimit,
		"retry-interval":     c.RetryInterval,
		"retry-max-interval": c.RetryMaxInterval,
		"purge-interval":     c.PurgeInterval,
		"batch-size":         c.BatchSize,
		"max-writes-pending": c.MaxWritesPending,
	}), nil
}
