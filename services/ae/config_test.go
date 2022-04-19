package ae_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/services/ae"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c ae.Config
	if _, err := toml.Decode(`
enabled = true
check-interval = "1s"
max-fetch = 20
max-sync = 2
auto-repair-missing = false
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if !c.Enabled {
		t.Fatalf("unexpected enabled state: %v", c.Enabled)
	} else if time.Duration(c.CheckInterval) != time.Second {
		t.Fatalf("unexpected check interval: %v", c.CheckInterval)
	} else if c.MaxFetch != 20 {
		t.Fatalf("unexpected max fetch: %v", c.MaxFetch)
	} else if c.MaxSync != 2 {
		t.Fatalf("unexpected max sync: %v", c.MaxSync)
	} else if c.AutoRepairMissing != false {
		t.Fatalf("unexpected auto repair missing: %v", c.AutoRepairMissing)
	}
}

func TestConfig_Validate(t *testing.T) {
	c := ae.NewConfig()
	c.Enabled = true
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected validation fail from NewConfig: %s", err)
	}

	c = ae.NewConfig()
	c.Enabled = true
	c.CheckInterval = 0
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for check-interval = 0, got nil")
	}

	c.Enabled = false
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected validation fail from disabled config: %s", err)
	}
}
