package run_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/cmd/influxd-meta/run"
)

func TestCommand_PIDFile(t *testing.T) {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "influxd-meta-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	pidFile := filepath.Join(tmpdir, "influxdb.pid")

	// Override the default meta dir so it doesn't look in ~/.influxdb which
	// might have junk not related to this test.
	os.Setenv("INFLUXDB_META_DIR", tmpdir)

	cmd := run.NewCommand()
	cmd.Getenv = func(key string) string {
		switch key {
		case "INFLUXDB_META_DIR":
			return filepath.Join(tmpdir, "meta")
		case "INFLUXDB_META_BIND_ADDRESS", "INFLUXDB_META_HTTP_BIND_ADDRESS":
			return "127.0.0.1:0"
		case "INFLUXDB_REPORTING_DISABLED":
			return "true"
		default:
			return os.Getenv(key)
		}
	}
	if err := cmd.Run("-pidfile", pidFile, "-config", os.DevNull, "-single-server"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if _, err := os.Stat(pidFile); err != nil {
		t.Fatalf("could not stat pid file: %s", err)
	}
	go cmd.Close()

	timeout := time.NewTimer(1000 * time.Millisecond)
	select {
	case <-timeout.C:
		t.Fatal("unexpected timeout")
	case <-cmd.Closed:
		timeout.Stop()
	}

	if _, err := os.Stat(pidFile); err == nil {
		t.Fatal("expected pid file to be removed")
	}
}
