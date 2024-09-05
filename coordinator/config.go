// Package coordinator contains abstractions for writing points, executing statements,
// and accessing meta data.
package coordinator

import (
	"crypto/tls"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultDialTimeout is the duration for which the meta node waits for a connection to a
	// remote data node before the meta node attempts to connect to a different remote data node.
	// This setting applies to queries only.
	DefaultDialTimeout = time.Second

	// DefaultPoolMaxIdleStreams is the maximum number of idle RPC stream connections to
	// retain in an idle pool between two nodes.
	DefaultPoolMaxIdleStreams = 100

	// DefaultPoolMaxIdleTime is the maximum time that a TCP connection to another data node
	// remains idle in the connection pool.
	DefaultPoolMaxIdleTime = time.Minute

	// DefaultShardReaderTimeout is the default timeout set on shard readers.
	DefaultShardReaderTimeout = time.Duration(0)

	// DefaultWriteTimeout is the default timeout for a complete write to succeed.
	DefaultWriteTimeout = 10 * time.Second

	// DefaultMaxConcurrentQueries is the maximum number of running queries.
	// A value of zero will make the maximum query limit unlimited.
	DefaultMaxConcurrentQueries = 0

	// DefaultMaxSelectPointN is the maximum number of points a SELECT can process.
	// A value of zero will make the maximum point count unlimited.
	DefaultMaxSelectPointN = 0

	// DefaultMaxSelectSeriesN is the maximum number of series a SELECT can run.
	// A value of zero will make the maximum series count unlimited.
	DefaultMaxSelectSeriesN = 0

	// DefaultMaxSelectBucketsN is the maximum number of group by time buckets a SELECT can create.
	// A value of 0 will make the maximum number of buckets unlimited.
	DefaultMaxSelectBucketsN = 0
)

// Config represents the configuration for the coordinator service.
type Config struct {
	DialTimeout           toml.Duration `toml:"dial-timeout"`
	PoolMaxIdleStreams    int           `toml:"pool-max-idle-streams"`
	PoolMaxIdleTime       toml.Duration `toml:"pool-max-idle-time"`
	AllowOutOfOrderWrites bool          `toml:"allow-out-of-order-writes"`
	ShardReaderTimeout    toml.Duration `toml:"shard-reader-timeout"`
	HTTPSEnabled          bool          `toml:"https-enabled"`
	HTTPSCertificate      string        `toml:"https-certificate"`
	HTTPSPrivateKey       string        `toml:"https-private-key"`
	HTTPSInsecureTLS      bool          `toml:"https-insecure-tls"`
	ClusterTracing        bool          `toml:"cluster-tracing"`
	WriteTimeout          toml.Duration `toml:"write-timeout"`
	MaxConcurrentQueries  int           `toml:"max-concurrent-queries"`
	QueryTimeout          toml.Duration `toml:"query-timeout"`
	LogQueriesAfter       toml.Duration `toml:"log-queries-after"`
	LogTimedOutQueries    bool          `toml:"log-timedout-queries"`
	MaxSelectPointN       int           `toml:"max-select-point"`
	MaxSelectSeriesN      int           `toml:"max-select-series"`
	MaxSelectBucketsN     int           `toml:"max-select-buckets"`
	TerminationQueryLog   bool          `toml:"termination-query-log"`

	// TLS is a base tls config to use for tls clients.
	TLS *tls.Config `toml:"-"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		DialTimeout:          toml.Duration(DefaultDialTimeout),
		PoolMaxIdleStreams:   DefaultPoolMaxIdleStreams,
		PoolMaxIdleTime:      toml.Duration(DefaultPoolMaxIdleTime),
		ShardReaderTimeout:   toml.Duration(DefaultShardReaderTimeout),
		WriteTimeout:         toml.Duration(DefaultWriteTimeout),
		QueryTimeout:         toml.Duration(query.DefaultQueryTimeout),
		MaxConcurrentQueries: DefaultMaxConcurrentQueries,
		LogTimedOutQueries:   false,
		MaxSelectPointN:      DefaultMaxSelectPointN,
		MaxSelectSeriesN:     DefaultMaxSelectSeriesN,
		MaxSelectBucketsN:    DefaultMaxSelectBucketsN,
		TerminationQueryLog:  false,
	}
}

// TLSConfig returns a TLS config.
func (c Config) TLSConfig() (*tls.Config, error) {
	return tcp.TLSConfig(c.TLS, c.HTTPSEnabled, c.HTTPSCertificate, c.HTTPSPrivateKey)
}

// TLSClientConfig returns a client TLS config.
func (c Config) TLSClientConfig() *tls.Config {
	return tcp.TLSClientConfig(c.HTTPSEnabled, c.HTTPSInsecureTLS)
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	return diagnostics.RowFromMap(map[string]interface{}{
		"dial-timeout":              c.DialTimeout,
		"pool-max-idle-streams":     c.PoolMaxIdleStreams,
		"pool-max-idle-time":        c.PoolMaxIdleTime,
		"allow-out-of-order-writes": c.AllowOutOfOrderWrites,
		"shard-reader-timeout":      c.ShardReaderTimeout,
		"cluster-tracing":           c.ClusterTracing,
		"write-timeout":             c.WriteTimeout,
		"max-concurrent-queries":    c.MaxConcurrentQueries,
		"query-timeout":             c.QueryTimeout,
		"log-queries-after":         c.LogQueriesAfter,
		"log-timedout-queries":      c.LogTimedOutQueries,
		"max-select-point":          c.MaxSelectPointN,
		"max-select-series":         c.MaxSelectSeriesN,
		"max-select-buckets":        c.MaxSelectBucketsN,
		"termination-query-log":     c.TerminationQueryLog,
	}), nil
}
