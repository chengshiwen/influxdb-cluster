package ae

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultCheckInterval is the interval of time when anti-entropy checks run on each data node.
	DefaultCheckInterval = 5 * time.Minute

	// DefaultMaxFetch is the maximum number of shards that a single data node
	// will copy or repair in parallel.
	DefaultMaxFetch = 10

	// DefaultMaxSync is the maximum number of concurrent sync operations
	// that should be performed.
	DefaultMaxSync = 1

	// DefaultAutoRepairMissing enables missing shards to automatically be repaired.
	DefaultAutoRepairMissing = true
)

// Config represents the configuration for the anti-entropy service.
type Config struct {
	Enabled           bool          `toml:"enabled"`
	CheckInterval     toml.Duration `toml:"check-interval"`
	MaxFetch          int           `toml:"max-fetch"`
	MaxSync           int           `toml:"max-sync"`
	AutoRepairMissing bool          `toml:"auto-repair-missing"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		Enabled:           false,
		CheckInterval:     toml.Duration(DefaultCheckInterval),
		MaxFetch:          DefaultMaxFetch,
		MaxSync:           DefaultMaxSync,
		AutoRepairMissing: DefaultAutoRepairMissing,
	}
}

// Validate returns an error if the Config is invalid.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	// TODO: Should we enforce a minimum interval?
	// Polling every nanosecond, for instance, will greatly impact performance.
	if c.CheckInterval <= 0 {
		return errors.New("check-interval must be positive")
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
		"enabled":             true,
		"check-interval":      c.CheckInterval,
		"max-fetch":           c.MaxFetch,
		"max-sync":            c.MaxSync,
		"auto-repair-missing": c.AutoRepairMissing,
	}), nil
}
