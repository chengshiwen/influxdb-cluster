package tcp_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tcp"
)

func TestListenTLSAndDialTLS(t *testing.T) {
	payload := []byte("listen tls and dial tls")

	l, err := tcp.ListenTLS("tcp", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer l.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			t.Errorf("error accepting tcp connection: %s", err)
			return
		}
		defer conn.Close()

		buf := &bytes.Buffer{}
		_, err = io.Copy(buf, conn)
		if err != nil {
			t.Errorf("error copying tcp connection: %s", err)
			return
		}

		if !reflect.DeepEqual(buf.Bytes(), payload) {
			t.Errorf("Unexpected payload: %s", buf.String())
		}
	}()

	conn, err := tcp.DialTLS("tcp", l.Addr().String(), nil)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	conn.Write(payload)
	conn.Close()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		t.Errorf("timeout while waiting for the goroutine")
	}
}
