package run_test

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/cmd/influxd-meta/run"
	influxtoml "github.com/influxdata/influxdb/toml"
	"go.uber.org/zap/zapcore"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Ensure the configuration can be parsed.
func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c run.Config
	if err := c.FromToml(`
reporting-disabled = true
bind-address = ":1000"
hostname = "local"

[meta]
dir = "/tmp/meta"
retention-autocreate = true
logging-enabled = true
bind-address = ":1010"
auth-enabled = true
ldap-allowed = true
http-bind-address = ":1020"
https-enabled = true
https-certificate = "/tmp/tls.crt"
https-private-key = "/tmp/tls.key"
https-insecure-tls = true
data-use-tls = true
data-insecure-tls = true
gossip-frequency = "15s"
announcement-expiration = "60s"
election-timeout = "2s"
heartbeat-timeout = "3s"
leader-lease-timeout = "600ms"
consensus-timeout = "40s"
commit-timeout = "60ms"
cluster-tracing = true
pprof-enabled = false
lease-duration = "2m0s"
shared-secret = "shared-secret"
internal-shared-secret = "internal-shared-secret"

[logging]
format = "logfmt"
level = "warn"
suppress-logo = true

[tls]
min-version = "tls1.2"
max-version = "tls1.3"
`); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if !c.ReportingDisabled {
		t.Fatalf("unexpected reporting disabled: %t", c.ReportingDisabled)
	} else if c.BindAddress != ":1000" {
		t.Fatalf("unexpected bind address: %s", c.BindAddress)
	} else if c.Hostname != "local" {
		t.Fatalf("unexpected hostname: %s", c.Hostname)
	} else if c.Meta.Dir != "/tmp/meta" {
		t.Fatalf("unexpected meta dir: %s", c.Meta.Dir)
	} else if !c.Meta.RetentionAutoCreate {
		t.Fatalf("unexpected retention autocreate: %t", c.Meta.RetentionAutoCreate)
	} else if !c.Meta.LoggingEnabled {
		t.Fatalf("unexpected logging enabled: %t", c.Meta.LoggingEnabled)
	} else if c.Meta.BindAddress != ":1010" {
		t.Fatalf("unexpected meta bind address: %s", c.Meta.BindAddress)
	} else if !c.Meta.AuthEnabled {
		t.Fatalf("unexpected auth enabled: %t", c.Meta.AuthEnabled)
	} else if !c.Meta.LDAPAllowed {
		t.Fatalf("unexpected ldap allowed: %t", c.Meta.LDAPAllowed)
	} else if c.Meta.HTTPBindAddress != ":1020" {
		t.Fatalf("unexpected http bind address: %s", c.Meta.HTTPBindAddress)
	} else if !c.Meta.HTTPSEnabled {
		t.Fatalf("unexpected https enabled: %t", c.Meta.HTTPSEnabled)
	} else if c.Meta.HTTPSCertificate != "/tmp/tls.crt" {
		t.Fatalf("unexpected https certificate: %s", c.Meta.HTTPSCertificate)
	} else if c.Meta.HTTPSPrivateKey != "/tmp/tls.key" {
		t.Fatalf("unexpected https private key: %s", c.Meta.HTTPSPrivateKey)
	} else if !c.Meta.HTTPSInsecureTLS {
		t.Fatalf("unexpected https insecure tls: %t", c.Meta.HTTPSInsecureTLS)
	} else if !c.Meta.DataUseTLS {
		t.Fatalf("unexpected data use tls: %t", c.Meta.DataUseTLS)
	} else if !c.Meta.DataInsecureTLS {
		t.Fatalf("unexpected data insecure tls: %t", c.Meta.DataInsecureTLS)
	} else if c.Meta.GossipFrequency != influxtoml.Duration(15*time.Second) {
		t.Fatalf("unexpected gossip frequency: %s", c.Meta.GossipFrequency)
	} else if c.Meta.AnnouncementExpiration != influxtoml.Duration(60*time.Second) {
		t.Fatalf("unexpected announcement expiration: %s", c.Meta.AnnouncementExpiration)
	} else if c.Meta.ElectionTimeout != influxtoml.Duration(2*time.Second) {
		t.Fatalf("unexpected election timeout: %s", c.Meta.ElectionTimeout)
	} else if c.Meta.HeartbeatTimeout != influxtoml.Duration(3*time.Second) {
		t.Fatalf("unexpected heartbeat timeout: %s", c.Meta.HeartbeatTimeout)
	} else if c.Meta.LeaderLeaseTimeout != influxtoml.Duration(600*time.Millisecond) {
		t.Fatalf("unexpected leader lease timeout: %s", c.Meta.LeaderLeaseTimeout)
	} else if c.Meta.ConsensusTimeout != influxtoml.Duration(40*time.Second) {
		t.Fatalf("unexpected consensus timeout: %s", c.Meta.ConsensusTimeout)
	} else if c.Meta.CommitTimeout != influxtoml.Duration(60*time.Millisecond) {
		t.Fatalf("unexpected commit timeout: %s", c.Meta.CommitTimeout)
	} else if !c.Meta.ClusterTracing {
		t.Fatalf("unexpected cluster tracing: %t", c.Meta.ClusterTracing)
	} else if c.Meta.PprofEnabled {
		t.Fatalf("unexpected pprof enabled: %t", c.Meta.PprofEnabled)
	} else if c.Meta.LeaseDuration != influxtoml.Duration(2*time.Minute) {
		t.Fatalf("unexpected lease duration: %s", c.Meta.LeaseDuration)
	} else if c.Meta.SharedSecret != "shared-secret" {
		t.Fatalf("unexpected shared secret: %s", c.Meta.SharedSecret)
	} else if c.Meta.InternalSharedSecret != "internal-shared-secret" {
		t.Fatalf("unexpected internal shared secret: %s", c.Meta.InternalSharedSecret)
	} else if c.Logging.Format != "logfmt" {
		t.Fatalf("unexpected logging format: %s", c.Logging.Format)
	} else if c.Logging.Level != zapcore.WarnLevel {
		t.Fatalf("unexpected logging level: %v", c.Logging.Level)
	} else if !c.Logging.SuppressLogo {
		t.Fatalf("unexpected logging format: %t", c.Logging.SuppressLogo)
	} else if c.TLS.MinVersion != "tls1.2" {
		t.Fatalf("unexpected tls min version: %v", c.TLS.MinVersion)
	} else if c.TLS.MaxVersion != "tls1.3" {
		t.Fatalf("unexpected tls max version: %v", c.TLS.MaxVersion)
	}
}

// Ensure the configuration can be parsed.
func TestConfig_Parse_EnvOverride(t *testing.T) {
	// Parse configuration.
	var c run.Config
	if _, err := toml.Decode(`
reporting-disabled = true
bind-address = ":1000"
hostname = "local"

[meta]
dir = "/tmp/meta"
retention-autocreate = true
logging-enabled = true
bind-address = ":1010"
auth-enabled = true
ldap-allowed = true
http-bind-address = ":1020"
https-enabled = true
https-certificate = "/tmp/tls.crt"
https-private-key = "/tmp/tls.key"
https-insecure-tls = true
data-use-tls = true
data-insecure-tls = true
gossip-frequency = "15s"
announcement-expiration = "60s"
election-timeout = "2s"
heartbeat-timeout = "3s"
leader-lease-timeout = "600ms"
consensus-timeout = "40s"
commit-timeout = "60ms"
cluster-tracing = true
pprof-enabled = false
lease-duration = "2m0s"
shared-secret = "shared-secret"
internal-shared-secret = "internal-shared-secret"

[logging]
format = "logfmt"
level = "warn"
suppress-logo = true

[tls]
min-version = "tls1.2"
max-version = "tls1.3"
`, &c); err != nil {
		t.Fatal(err)
	}

	getenv := func(s string) string {
		switch s {
		case "INFLUXDB_HOSTNAME":
			return "localhost"
		case "INFLUXDB_META_DIR":
			return "/var/lib/influxdb/meta"
		case "INFLUXDB_META_RETENTION_AUTOCREATE":
			return "false"
		case "INFLUXDB_META_LOGGING_ENABLED":
			return "false"
		case "INFLUXDB_META_AUTH_ENABLED":
			return "false"
		case "INFLUXDB_META_LDAP_ALLOWED":
			return "false"
		case "INFLUXDB_META_BIND_ADDRESS":
			return ":1234"
		case "INFLUXDB_META_HTTP_BIND_ADDRESS":
			return ":5555"
		case "INFLUXDB_META_LEADER_LEASE_TIMEOUT":
			return "400ms"
		case "INFLUXDB_META_CONSENSUS_TIMEOUT":
			return "20s"
		case "INFLUXDB_META_COMMIT_TIMEOUT":
			return "40ms"
		case "INFLUXDB_META_CLUSTER_TRACING":
			return "false"
		case "INFLUXDB_META_SHARED_SECRET":
			return "meta-shared-secret"
		case "INFLUXDB_LOGGING_FORMAT":
			return "json"
		case "INFLUXDB_LOGGING_LEVEL":
			// logging type
			return "error"
		case "INFLUXDB_TLS_MIN_VERSION":
			return "tls1.1"
		}
		return ""
	}

	if err := c.ApplyEnvOverrides(getenv); err != nil {
		t.Fatalf("failed to apply env overrides: %v", err)
	}

	// Validate configuration.
	if c.Hostname != "localhost" {
		t.Fatalf("unexpected hostname: %s", c.Hostname)
	} else if c.Meta.Dir != "/var/lib/influxdb/meta" {
		t.Fatalf("unexpected meta dir: %s", c.Meta.Dir)
	} else if c.Meta.RetentionAutoCreate {
		t.Fatalf("unexpected retention autocreate: %t", c.Meta.RetentionAutoCreate)
	} else if c.Meta.LoggingEnabled {
		t.Fatalf("unexpected logging enabled: %t", c.Meta.LoggingEnabled)
	} else if c.Meta.BindAddress != ":1234" {
		t.Fatalf("unexpected meta bind address: %s", c.Meta.BindAddress)
	} else if c.Meta.AuthEnabled {
		t.Fatalf("unexpected auth enabled: %t", c.Meta.AuthEnabled)
	} else if c.Meta.LDAPAllowed {
		t.Fatalf("unexpected ldap allowed: %t", c.Meta.LDAPAllowed)
	} else if c.Meta.HTTPBindAddress != ":5555" {
		t.Fatalf("unexpected http bind address: %s", c.Meta.HTTPBindAddress)
	} else if c.Meta.LeaderLeaseTimeout != influxtoml.Duration(400*time.Millisecond) {
		t.Fatalf("unexpected leader lease timeout: %s", c.Meta.LeaderLeaseTimeout)
	} else if c.Meta.ConsensusTimeout != influxtoml.Duration(20*time.Second) {
		t.Fatalf("unexpected consensus timeout: %s", c.Meta.ConsensusTimeout)
	} else if c.Meta.CommitTimeout != influxtoml.Duration(40*time.Millisecond) {
		t.Fatalf("unexpected commit timeout: %s", c.Meta.CommitTimeout)
	} else if c.Meta.ClusterTracing {
		t.Fatalf("unexpected cluster tracing: %t", c.Meta.ClusterTracing)
	} else if c.Meta.SharedSecret != "meta-shared-secret" {
		t.Fatalf("unexpected shared secret: %s", c.Meta.SharedSecret)
	} else if c.Logging.Format != "json" {
		t.Fatalf("unexpected logging format: %s", c.Logging.Format)
	} else if c.Logging.Level != zapcore.ErrorLevel {
		t.Fatalf("unexpected logging level: %v", c.Logging.Level)
	} else if c.TLS.MinVersion != "tls1.1" {
		t.Fatalf("unexpected tls min version: %v", c.TLS.MinVersion)
	}
}

// Ensure that Config.Validate correctly validates the individual subsections.
func TestConfig_InvalidSubsections(t *testing.T) {
	// Precondition: NewDemoConfig must validate correctly.
	c, err := run.NewDemoConfig()
	if err != nil {
		t.Fatalf("error creating demo config: %s", err)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("new demo config failed validation: %s", err)
	}

	// For each subsection, load a config with a single invalid setting.
	for _, tc := range []struct {
		section string
		kv      string
	}{
		{"meta", `dir = ""`},
	} {
		c, err := run.NewDemoConfig()
		if err != nil {
			t.Fatalf("error creating demo config: %s", err)
		}

		s := fmt.Sprintf("\n[%s]\n%s\n", tc.section, tc.kv)
		if err := c.FromToml(s); err != nil {
			t.Fatalf("error loading toml %q: %s", s, err)
		}

		if err := c.Validate(); err == nil {
			t.Fatalf("expected error but got nil for config: %s", s)
		}
	}
}

// Ensure the configuration can be parsed when a Byte-Order-Mark is present.
func TestConfig_Parse_UTF8_ByteOrderMark(t *testing.T) {
	// Parse configuration.
	var c run.Config
	f, err := os.CreateTemp("", "influxd")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	f.WriteString("\ufeff")
	f.WriteString(`
reporting-disabled = true
bind-address = ":1000"
hostname = "local"

[meta]
dir = "/tmp/meta"
retention-autocreate = true
logging-enabled = true
bind-address = ":1010"
auth-enabled = true
ldap-allowed = true
http-bind-address = ":1020"
https-enabled = true
https-certificate = "/tmp/tls.crt"
https-private-key = "/tmp/tls.key"
https-insecure-tls = true
data-use-tls = true
data-insecure-tls = true
gossip-frequency = "15s"
announcement-expiration = "60s"
election-timeout = "2s"
heartbeat-timeout = "3s"
leader-lease-timeout = "600ms"
consensus-timeout = "40s"
commit-timeout = "60ms"
cluster-tracing = true
pprof-enabled = false
lease-duration = "2m0s"
shared-secret = "shared-secret"
internal-shared-secret = "internal-shared-secret"

[logging]
format = "logfmt"
level = "warn"
suppress-logo = true

[tls]
min-version = "tls1.2"
max-version = "tls1.3"
`)
	if err := c.FromTomlFile(f.Name()); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	// Validate configuration.
	if !c.ReportingDisabled {
		t.Fatalf("unexpected reporting disabled: %t", c.ReportingDisabled)
	} else if c.BindAddress != ":1000" {
		t.Fatalf("unexpected bind address: %s", c.BindAddress)
	} else if c.Hostname != "local" {
		t.Fatalf("unexpected hostname: %s", c.Hostname)
	} else if c.Meta.Dir != "/tmp/meta" {
		t.Fatalf("unexpected meta dir: %s", c.Meta.Dir)
	} else if !c.Meta.RetentionAutoCreate {
		t.Fatalf("unexpected retention autocreate: %t", c.Meta.RetentionAutoCreate)
	} else if !c.Meta.LoggingEnabled {
		t.Fatalf("unexpected logging enabled: %t", c.Meta.LoggingEnabled)
	} else if c.Meta.BindAddress != ":1010" {
		t.Fatalf("unexpected meta bind address: %s", c.Meta.BindAddress)
	} else if !c.Meta.AuthEnabled {
		t.Fatalf("unexpected auth enabled: %t", c.Meta.AuthEnabled)
	} else if !c.Meta.LDAPAllowed {
		t.Fatalf("unexpected ldap allowed: %t", c.Meta.LDAPAllowed)
	} else if c.Meta.HTTPBindAddress != ":1020" {
		t.Fatalf("unexpected http bind address: %s", c.Meta.HTTPBindAddress)
	} else if !c.Meta.HTTPSEnabled {
		t.Fatalf("unexpected https enabled: %t", c.Meta.HTTPSEnabled)
	} else if c.Meta.HTTPSCertificate != "/tmp/tls.crt" {
		t.Fatalf("unexpected https certificate: %s", c.Meta.HTTPSCertificate)
	} else if c.Meta.HTTPSPrivateKey != "/tmp/tls.key" {
		t.Fatalf("unexpected https private key: %s", c.Meta.HTTPSPrivateKey)
	} else if !c.Meta.HTTPSInsecureTLS {
		t.Fatalf("unexpected https insecure tls: %t", c.Meta.HTTPSInsecureTLS)
	} else if !c.Meta.DataUseTLS {
		t.Fatalf("unexpected data use tls: %t", c.Meta.DataUseTLS)
	} else if !c.Meta.DataInsecureTLS {
		t.Fatalf("unexpected data insecure tls: %t", c.Meta.DataInsecureTLS)
	} else if c.Meta.GossipFrequency != influxtoml.Duration(15*time.Second) {
		t.Fatalf("unexpected gossip frequency: %s", c.Meta.GossipFrequency)
	} else if c.Meta.AnnouncementExpiration != influxtoml.Duration(60*time.Second) {
		t.Fatalf("unexpected announcement expiration: %s", c.Meta.AnnouncementExpiration)
	} else if c.Meta.ElectionTimeout != influxtoml.Duration(2*time.Second) {
		t.Fatalf("unexpected election timeout: %s", c.Meta.ElectionTimeout)
	} else if c.Meta.HeartbeatTimeout != influxtoml.Duration(3*time.Second) {
		t.Fatalf("unexpected heartbeat timeout: %s", c.Meta.HeartbeatTimeout)
	} else if c.Meta.LeaderLeaseTimeout != influxtoml.Duration(600*time.Millisecond) {
		t.Fatalf("unexpected leader lease timeout: %s", c.Meta.LeaderLeaseTimeout)
	} else if c.Meta.ConsensusTimeout != influxtoml.Duration(40*time.Second) {
		t.Fatalf("unexpected consensus timeout: %s", c.Meta.ConsensusTimeout)
	} else if c.Meta.CommitTimeout != influxtoml.Duration(60*time.Millisecond) {
		t.Fatalf("unexpected commit timeout: %s", c.Meta.CommitTimeout)
	} else if !c.Meta.ClusterTracing {
		t.Fatalf("unexpected cluster tracing: %t", c.Meta.ClusterTracing)
	} else if c.Meta.PprofEnabled {
		t.Fatalf("unexpected pprof enabled: %t", c.Meta.PprofEnabled)
	} else if c.Meta.LeaseDuration != influxtoml.Duration(2*time.Minute) {
		t.Fatalf("unexpected lease duration: %s", c.Meta.LeaseDuration)
	} else if c.Meta.SharedSecret != "shared-secret" {
		t.Fatalf("unexpected shared secret: %s", c.Meta.SharedSecret)
	} else if c.Meta.InternalSharedSecret != "internal-shared-secret" {
		t.Fatalf("unexpected internal shared secret: %s", c.Meta.InternalSharedSecret)
	} else if c.Logging.Format != "logfmt" {
		t.Fatalf("unexpected logging format: %s", c.Logging.Format)
	} else if c.Logging.Level != zapcore.WarnLevel {
		t.Fatalf("unexpected logging level: %v", c.Logging.Level)
	} else if !c.Logging.SuppressLogo {
		t.Fatalf("unexpected logging format: %t", c.Logging.SuppressLogo)
	} else if c.TLS.MinVersion != "tls1.2" {
		t.Fatalf("unexpected tls min version: %v", c.TLS.MinVersion)
	} else if c.TLS.MaxVersion != "tls1.3" {
		t.Fatalf("unexpected tls max version: %v", c.TLS.MaxVersion)
	}
}

// Ensure the configuration can be parsed when a Byte-Order-Mark is present.
func TestConfig_Parse_UTF16_ByteOrderMark(t *testing.T) {
	// Parse configuration.
	var c run.Config
	f, err := os.CreateTemp("", "influxd")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	utf16 := unicode.UTF16(unicode.BigEndian, unicode.UseBOM)
	w := transform.NewWriter(f, utf16.NewEncoder())
	io.WriteString(w, `
reporting-disabled = true
bind-address = ":1000"
hostname = "local"

[meta]
dir = "/tmp/meta"
retention-autocreate = true
logging-enabled = true
bind-address = ":1010"
auth-enabled = true
ldap-allowed = true
http-bind-address = ":1020"
https-enabled = true
https-certificate = "/tmp/tls.crt"
https-private-key = "/tmp/tls.key"
https-insecure-tls = true
data-use-tls = true
data-insecure-tls = true
gossip-frequency = "15s"
announcement-expiration = "60s"
election-timeout = "2s"
heartbeat-timeout = "3s"
leader-lease-timeout = "600ms"
consensus-timeout = "40s"
commit-timeout = "60ms"
cluster-tracing = true
pprof-enabled = false
lease-duration = "2m0s"
shared-secret = "shared-secret"
internal-shared-secret = "internal-shared-secret"

[logging]
format = "logfmt"
level = "warn"
suppress-logo = true

[tls]
min-version = "tls1.2"
max-version = "tls1.3"
`)
	if err := c.FromTomlFile(f.Name()); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if !c.ReportingDisabled {
		t.Fatalf("unexpected reporting disabled: %t", c.ReportingDisabled)
	} else if c.BindAddress != ":1000" {
		t.Fatalf("unexpected bind address: %s", c.BindAddress)
	} else if c.Hostname != "local" {
		t.Fatalf("unexpected hostname: %s", c.Hostname)
	} else if c.Meta.Dir != "/tmp/meta" {
		t.Fatalf("unexpected meta dir: %s", c.Meta.Dir)
	} else if !c.Meta.RetentionAutoCreate {
		t.Fatalf("unexpected retention autocreate: %t", c.Meta.RetentionAutoCreate)
	} else if !c.Meta.LoggingEnabled {
		t.Fatalf("unexpected logging enabled: %t", c.Meta.LoggingEnabled)
	} else if c.Meta.BindAddress != ":1010" {
		t.Fatalf("unexpected meta bind address: %s", c.Meta.BindAddress)
	} else if !c.Meta.AuthEnabled {
		t.Fatalf("unexpected auth enabled: %t", c.Meta.AuthEnabled)
	} else if !c.Meta.LDAPAllowed {
		t.Fatalf("unexpected ldap allowed: %t", c.Meta.LDAPAllowed)
	} else if c.Meta.HTTPBindAddress != ":1020" {
		t.Fatalf("unexpected http bind address: %s", c.Meta.HTTPBindAddress)
	} else if !c.Meta.HTTPSEnabled {
		t.Fatalf("unexpected https enabled: %t", c.Meta.HTTPSEnabled)
	} else if c.Meta.HTTPSCertificate != "/tmp/tls.crt" {
		t.Fatalf("unexpected https certificate: %s", c.Meta.HTTPSCertificate)
	} else if c.Meta.HTTPSPrivateKey != "/tmp/tls.key" {
		t.Fatalf("unexpected https private key: %s", c.Meta.HTTPSPrivateKey)
	} else if !c.Meta.HTTPSInsecureTLS {
		t.Fatalf("unexpected https insecure tls: %t", c.Meta.HTTPSInsecureTLS)
	} else if !c.Meta.DataUseTLS {
		t.Fatalf("unexpected data use tls: %t", c.Meta.DataUseTLS)
	} else if !c.Meta.DataInsecureTLS {
		t.Fatalf("unexpected data insecure tls: %t", c.Meta.DataInsecureTLS)
	} else if c.Meta.GossipFrequency != influxtoml.Duration(15*time.Second) {
		t.Fatalf("unexpected gossip frequency: %s", c.Meta.GossipFrequency)
	} else if c.Meta.AnnouncementExpiration != influxtoml.Duration(60*time.Second) {
		t.Fatalf("unexpected announcement expiration: %s", c.Meta.AnnouncementExpiration)
	} else if c.Meta.ElectionTimeout != influxtoml.Duration(2*time.Second) {
		t.Fatalf("unexpected election timeout: %s", c.Meta.ElectionTimeout)
	} else if c.Meta.HeartbeatTimeout != influxtoml.Duration(3*time.Second) {
		t.Fatalf("unexpected heartbeat timeout: %s", c.Meta.HeartbeatTimeout)
	} else if c.Meta.LeaderLeaseTimeout != influxtoml.Duration(600*time.Millisecond) {
		t.Fatalf("unexpected leader lease timeout: %s", c.Meta.LeaderLeaseTimeout)
	} else if c.Meta.ConsensusTimeout != influxtoml.Duration(40*time.Second) {
		t.Fatalf("unexpected consensus timeout: %s", c.Meta.ConsensusTimeout)
	} else if c.Meta.CommitTimeout != influxtoml.Duration(60*time.Millisecond) {
		t.Fatalf("unexpected commit timeout: %s", c.Meta.CommitTimeout)
	} else if !c.Meta.ClusterTracing {
		t.Fatalf("unexpected cluster tracing: %t", c.Meta.ClusterTracing)
	} else if c.Meta.PprofEnabled {
		t.Fatalf("unexpected pprof enabled: %t", c.Meta.PprofEnabled)
	} else if c.Meta.LeaseDuration != influxtoml.Duration(2*time.Minute) {
		t.Fatalf("unexpected lease duration: %s", c.Meta.LeaseDuration)
	} else if c.Meta.SharedSecret != "shared-secret" {
		t.Fatalf("unexpected shared secret: %s", c.Meta.SharedSecret)
	} else if c.Meta.InternalSharedSecret != "internal-shared-secret" {
		t.Fatalf("unexpected internal shared secret: %s", c.Meta.InternalSharedSecret)
	} else if c.Logging.Format != "logfmt" {
		t.Fatalf("unexpected logging format: %s", c.Logging.Format)
	} else if c.Logging.Level != zapcore.WarnLevel {
		t.Fatalf("unexpected logging level: %v", c.Logging.Level)
	} else if !c.Logging.SuppressLogo {
		t.Fatalf("unexpected logging format: %t", c.Logging.SuppressLogo)
	} else if c.TLS.MinVersion != "tls1.2" {
		t.Fatalf("unexpected tls min version: %v", c.TLS.MinVersion)
	} else if c.TLS.MaxVersion != "tls1.3" {
		t.Fatalf("unexpected tls max version: %v", c.TLS.MaxVersion)
	}
}
