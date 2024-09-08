package coordinator

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	// ErrClosed is the error resulting if the pool is closed via pool.Close().
	ErrClosed = errors.New("pool is closed")

	// PoolWaitTimeout is the timeout waiting for free connection.
	PoolWaitTimeout = 5 * time.Second
)

// Pool interface describes a pool implementation. A pool should have maximum
// capacity. An ideal pool is thread-safe and easy to use.
type Pool interface {
	// Get returns a new connection from the pool. Closing the connections puts
	// it back to the Pool. Closing it when the pool is destroyed or full will
	// be counted as an error.
	Get() (net.Conn, error)

	// Close closes the pool and all its connections. After Close() the pool is
	// no longer usable.
	Close()

	// Len returns the current number of idle connections of the pool.
	Len() int

	// Size returns the total number of alive connections of the pool.
	Size() int
}

// idleConn implements idle connection.
type idleConn struct {
	c net.Conn
	t time.Time
}

// boundedPool implements the Pool interface based on buffered channels.
type boundedPool struct {
	// storage for our net.Conn connections
	mu    sync.RWMutex
	conns chan *idleConn

	total chan struct{}
	done  chan struct{}
	// net.Conn generator
	factory Factory
}

// Factory is a function to create new connections.
type Factory func() (net.Conn, error)

// NewBoundedPool returns a new pool based on buffered channels with an initial
// capacity, maximum capacity and maximum idle time that a connection remains
// idle in the connection pool. Factory is used when initial capacity is
// greater than zero to fill the pool. A zero initialCap doesn't fill the Pool
// until a new Get() is called. During a Get(), If there is no new connection
// available in the pool and total connections is less than maxCap, a new connection
// will be created via the Factory() method. Otherwise, the call will block until
// a connection is available or the timeout is reached.
func NewBoundedPool(initialCap, maxCap int, idleTime time.Duration, factory Factory) (Pool, error) {
	if initialCap < 0 || maxCap <= 0 || initialCap > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	c := &boundedPool{
		conns:   make(chan *idleConn, maxCap),
		total:   make(chan struct{}, maxCap),
		done:    make(chan struct{}),
		factory: factory,
	}

	// create initial connections, if something goes wrong,
	// just close the pool error out.
	for i := 0; i < initialCap; i++ {
		conn, err := factory()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- &idleConn{c: conn, t: time.Now()}
		c.total <- struct{}{}
	}

	go c.pruneIdleConns(idleTime)
	return c, nil
}

func (c *boundedPool) getConnsAndFactory() (chan *idleConn, Factory) {
	c.mu.RLock()
	conns := c.conns
	factory := c.factory
	c.mu.RUnlock()
	return conns, factory
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (c *boundedPool) Get() (net.Conn, error) {
	conns, factory := c.getConnsAndFactory()
	if conns == nil {
		return nil, ErrClosed
	}

	// Try and grab a connection from the pool
	// Wrap our connections with our custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}
		return c.wrapConn(conn.c), nil
	default:
		// Could not get connection, can we create a new one?
		if c.tryTake() {
			conn, err := factory()
			if err != nil {
				c.tryFree()
				return nil, err
			}
			return c.wrapConn(conn), nil
		}
	}

	// The pool was empty and we couldn't create a new one to
	// retry until one is free or we timeout
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}
		return c.wrapConn(conn.c), nil
	case <-time.After(PoolWaitTimeout):
		return nil, errors.New("timed out waiting for free connection")
	}
}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *boundedPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conns == nil {
		// pool is closed, close passed connection
		return conn.Close()
	}

	// put the resource back into the pool. If the pool is full, this will
	// block and the default case will be executed.
	select {
	case c.conns <- &idleConn{c: conn, t: time.Now()}:
		return nil
	default:
		// pool is full, close passed connection
		c.tryFree()
		return conn.Close()
	}
}

func (c *boundedPool) Close() {
	c.mu.Lock()
	conns, total, done := c.conns, c.total, c.done
	c.conns = nil
	c.total = nil
	c.done = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.c.Close()
	}
	close(total)
	close(done)
}

func (c *boundedPool) Len() int {
	conns, _ := c.getConnsAndFactory()
	return len(conns)
}

func (c *boundedPool) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.total)
}

func (c *boundedPool) tryTake() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	select {
	case c.total <- struct{}{}:
		return true
	default:
	}
	return false
}

func (c *boundedPool) tryFree() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	select {
	case <-c.total:
		return true
	default:
	}
	return false
}

// pruneIdleConns prunes idle connections.
func (c *boundedPool) pruneIdleConns(idleTime time.Duration) {
	if idleTime <= 0 {
		return
	}
	ticker := time.NewTicker(idleTime)
	defer ticker.Stop()
	for {
		c.mu.RLock()
		done := c.done
		c.mu.RUnlock()
		select {
		case <-done:
			return
		case <-ticker.C:
			conns, _ := c.getConnsAndFactory()
			if conns == nil {
				return
			}
			if len(conns) == 0 {
				continue
			}
			var newConns []*idleConn
			for {
				select {
				case conn := <-conns:
					if conn.t.Add(idleTime).Before(time.Now()) {
						c.tryFree()
						conn.c.Close()
					} else {
						newConns = append(newConns, conn)
					}
				default:
					goto DONE
				}
			}
		DONE:
			if len(newConns) > 0 {
				c.mu.RLock()
				for _, conn := range newConns {
					c.conns <- conn
				}
				c.mu.RUnlock()
				newConns = nil
			}
		}
	}
}

// wrapConn wraps a standard net.Conn to a poolConn net.Conn.
func (c *boundedPool) wrapConn(conn net.Conn) net.Conn {
	p := &pooledConn{c: c}
	p.Conn = conn
	return p
}

// pooledConn is a wrapper around net.Conn to modify the behavior of
// net.Conn's Close() method.
type pooledConn struct {
	net.Conn
	mu       sync.RWMutex
	c        *boundedPool
	unusable bool
}

// Close puts the given connects back to the pool instead of closing it.
func (p *pooledConn) Close() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.unusable {
		if p.Conn != nil {
			p.c.tryFree()
			return p.Conn.Close()
		}
		return nil
	}
	return p.c.put(p.Conn)
}

// MarkUnusable marks the connection not usable any more, to let the pool close it instead of returning it to pool.
func (p *pooledConn) MarkUnusable() {
	p.mu.Lock()
	p.unusable = true
	p.mu.Unlock()
}

func MarkUnusable(conn net.Conn) {
	if pc, ok := conn.(*pooledConn); ok {
		pc.MarkUnusable()
	}
}
