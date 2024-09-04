package coordinator

import (
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	InitialCap = 5
	MaximumCap = 30
	IdleTime   = 60 * time.Second
	network    = "tcp"
	address    = "127.0.0.1:7777"
	factory    = func() (net.Conn, error) { return net.Dial(network, address) }
)

func init() {
	// used for factory function
	go simpleTCPServer()
	time.Sleep(time.Millisecond * 300) // wait until tcp server has been settled

	rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
}

func TestNew(t *testing.T) {
	p, err := newBoundedPool()
	if err != nil {
		t.Errorf("New pool error: %s", err)
	}
	defer p.Close()
}

func TestPool_Get_Impl(t *testing.T) {
	p, _ := newBoundedPool()
	defer p.Close()

	conn, err := p.Get()
	if err != nil {
		t.Errorf("Get error: %s", err)
	}

	_, ok := conn.(*pooledConn)
	if !ok {
		t.Errorf("Conn is not of type poolConn")
	}
}

func TestPool_Get(t *testing.T) {
	p, _ := newBoundedPool()
	defer p.Close()

	_, err := p.Get()
	if err != nil {
		t.Errorf("Get error: %s", err)
	}

	// after one get, current capacity should be lowered by one.
	if p.Len() != (InitialCap - 1) {
		t.Errorf("Get error. Expecting %d, got %d", InitialCap-1, p.Len())
	}

	// get them all
	var wg sync.WaitGroup
	for i := 0; i < (InitialCap - 1); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Get()
			if err != nil {
				t.Errorf("Get error: %s", err)
			}
		}()
	}
	wg.Wait()

	if p.Len() != 0 {
		t.Errorf("Get error. Expecting %d, got %d", InitialCap-1, p.Len())
	}

	_, err = p.Get()
	if err != nil {
		t.Errorf("Get error: %s", err)
	}
}

func TestPool_Put(t *testing.T) {
	p, err := NewBoundedPool(0, 30, IdleTime, factory)
	if err != nil {
		t.Error(err)
	}
	defer p.Close()

	// get/create from the pool
	conns := make([]net.Conn, MaximumCap)
	for i := 0; i < MaximumCap; i++ {
		conn, _ := p.Get()
		conns[i] = conn
	}

	// now put them all back
	for _, conn := range conns {
		conn.Close()
	}

	if p.Len() != MaximumCap {
		t.Errorf("Put error len. Expecting %d, got %d", 1, p.Len())
	}

	conn, _ := p.Get()
	p.Close() // close pool

	conn.Close() // try to put into a full pool
	if p.Len() != 0 {
		t.Errorf("Put error. Closed pool shouldn't allow to put connections.")
	}
}

func TestPool_PutUnusableConn(t *testing.T) {
	p, _ := newBoundedPool()
	defer p.Close()

	// ensure pool is not empty
	conn, _ := p.Get()
	conn.Close()

	poolSize := p.Len()
	conn, _ = p.Get()
	conn.Close()
	if p.Len() != poolSize {
		t.Errorf("Pool size is expected to be equal to initial size")
	}

	conn, _ = p.Get()
	MarkUnusable(conn)
	conn.Close()
	if p.Len() != poolSize-1 {
		t.Errorf("Pool size is expected to be initial_size - 1. Expecting %d, got %d", poolSize-1, p.Len())
	}
}

func TestPool_UsedCapacity(t *testing.T) {
	p, _ := newBoundedPool()
	defer p.Close()

	if p.Len() != InitialCap {
		t.Errorf("InitialCap error. Expecting %d, got %d", InitialCap, p.Len())
	}
}

func TestPool_Close(t *testing.T) {
	p, _ := newBoundedPool()

	// now close it and test all cases we are expecting.
	p.Close()

	c := p.(*boundedPool)

	if c.conns != nil {
		t.Errorf("Close error, conns channel should be nil")
	}

	if c.factory != nil {
		t.Errorf("Close error, factory should be nil")
	}

	_, err := p.Get()
	if err == nil {
		t.Errorf("Close error, get conn should return an error")
	}

	if p.Len() != 0 {
		t.Errorf("Close error used capacity. Expecting 0, got %d", p.Len())
	}
}

func TestPoolConcurrent(t *testing.T) {
	p, _ := newBoundedPool()
	pipe := make(chan net.Conn, 0)

	go func() {
		p.Close()
	}()

	for i := 0; i < MaximumCap; i++ {
		go func() {
			conn, _ := p.Get()
			pipe <- conn
		}()

		go func() {
			conn := <-pipe
			if conn == nil {
				return
			}
			conn.Close()
		}()
	}
}

func TestPoolWriteRead(t *testing.T) {
	p, _ := NewBoundedPool(0, 30, IdleTime, factory)
	defer p.Close()

	conn, _ := p.Get()

	msg := "hello"
	_, err := conn.Write([]byte(msg))
	if err != nil {
		t.Error(err)
	}
}

func TestPoolConcurrent2(t *testing.T) {
	p, _ := NewBoundedPool(0, 30, IdleTime, factory)
	defer p.Close()

	var wg sync.WaitGroup

	go func() {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				conn, _ := p.Get()
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
				conn.Close()
				wg.Done()
			}(i)
		}
	}()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			conn, _ := p.Get()
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
			conn.Close()
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestPoolConcurrent3(t *testing.T) {
	p, _ := NewBoundedPool(0, 1, IdleTime, factory)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		p.Close()
		wg.Done()
	}()

	if conn, err := p.Get(); err == nil {
		conn.Close()
	}

	wg.Wait()
}

func TestPool_GetWaitTimeout(t *testing.T) {
	maxCap := 1
	p, err := NewBoundedPool(0, maxCap, IdleTime, factory)
	if err != nil {
		t.Errorf("New pool error: %s", err)
	}
	defer p.Close()

	for i := 0; i < maxCap+1; i++ {
		_, err := p.Get()
		if i < maxCap {
			if err != nil {
				t.Errorf("Expected nil, got: %s", err)
			}
		} else {
			if err == nil || err.Error() != "timed out waiting for free connection" {
				t.Errorf("Get timeout error. Expecting ‘timed out waiting for free connection’, got %s", err)
			}
		}
	}
}

func TestPool_PruneIdleConns(t *testing.T) {
	p, _ := NewBoundedPool(InitialCap, MaximumCap, 1*time.Second, factory)
	defer p.Close()

	var wg sync.WaitGroup
	numWorkers := MaximumCap * 2
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := p.Get()
			if err != nil {
				return
			}
			defer conn.Close()
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
		}()
	}

	wg.Wait()
	time.Sleep(2 * time.Second)

	if p.Len() != 0 {
		t.Errorf("Idle connections expected: 0, got: %d", p.Len())
	}

	if p.Size() != 0 {
		t.Errorf("Opened connections expected: 0, got %d", p.Size())
	}
}

func TestPoolMaximumCapacity(t *testing.T) {
	var success int32
	p, _ := NewBoundedPool(InitialCap, MaximumCap, IdleTime, factory)
	defer p.Close()

	var wg sync.WaitGroup
	numWorkers := MaximumCap * 2
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, err := p.Get()
			if err != nil {
				return
			}
			atomic.AddInt32(&success, 1)
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&success) != int32(MaximumCap) {
		t.Errorf("Opened success connections expected: %d, got: %d", MaximumCap, success)
	}

	if p.Size() != MaximumCap {
		t.Errorf("Opened connections expected: %d, got %d", MaximumCap, p.Size())
	}
}

func TestPoolMaximumCapacity_Close(t *testing.T) {
	var success int32
	p, _ := NewBoundedPool(InitialCap, MaximumCap, IdleTime, factory)
	defer p.Close()

	var wg sync.WaitGroup
	numWorkers := MaximumCap * 2
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := p.Get()
			if err != nil {
				t.Errorf("Expected nil, got: %s", err)
				return
			}
			defer conn.Close()

			atomic.AddInt32(&success, 1)
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&success) != int32(numWorkers) {
		t.Errorf("Opened success connections expected: %d, got: %d", numWorkers, atomic.LoadInt32(&success))
	}
}

func TestPool_Get1(t *testing.T) {
	p, _ := NewBoundedPool(0, 1, IdleTime, factory)
	defer p.Close()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := p.Get()
			if err != nil {
				t.Errorf("Expected nil, got: %s", err)
				return
			}
			defer conn.Close()

			time.Sleep(time.Millisecond * 100)
		}()
	}
	wg.Wait()
}

func TestPool_ClosedConnectionMarkUnusable(t *testing.T) {
	p, _ := NewBoundedPool(1, 1, IdleTime, factory)
	defer p.Close()

	conn, _ := p.Get()

	conn.(*pooledConn).MarkUnusable()
	conn.Close()

	conn, err := p.Get()
	if err != nil {
		t.Errorf("Expected nil, got: %s", err)
	}
	if conn == nil {
		t.Errorf("Expected non-nil connection")
	}
	conn.Close()
}

func TestPool_FailedFactoryNotOpenConnections(t *testing.T) {
	factory := func() (net.Conn, error) {
		return net.DialTimeout("tcp", "localhost:1234", time.Millisecond)
	}

	maxCap := 2
	p, err := NewBoundedPool(0, maxCap, IdleTime, factory)
	if err != nil {
		t.Errorf("New pool error: %s", err)
	}
	defer p.Close()

	for i := 0; i < maxCap+1; i++ {
		conn, err := p.Get()
		if err == nil && conn != nil {
			conn.Close()
		}
	}

	if p.Size() != 0 {
		t.Errorf("Failed factory shouldn't open connections. Expecting 0, got %d", p.Size())
	}
}

func TestConn_Impl(t *testing.T) {
	var _ net.Conn = new(pooledConn)
}

func newBoundedPool() (Pool, error) {
	return NewBoundedPool(InitialCap, MaximumCap, IdleTime, factory)
}

func simpleTCPServer() {
	l, err := net.Listen(network, address)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go func() {
			buffer := make([]byte, 256)
			conn.Read(buffer)
		}()
	}
}
