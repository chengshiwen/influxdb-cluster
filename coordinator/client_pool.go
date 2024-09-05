package coordinator

import (
	"errors"
	"net"
	"sync"
)

var ErrClientClosed = errors.New("client already closed")

type clientPool struct {
	mu   sync.RWMutex
	pool map[uint64]Pool
}

func newClientPool() *clientPool {
	return &clientPool{
		pool: make(map[uint64]Pool),
	}
}

func (c *clientPool) setPool(nodeID uint64, p Pool) {
	c.mu.Lock()
	c.pool[nodeID] = p
	c.mu.Unlock()
}

func (c *clientPool) getPool(nodeID uint64) (Pool, bool) {
	c.mu.RLock()
	p, ok := c.pool[nodeID]
	c.mu.RUnlock()
	return p, ok
}

func (c *clientPool) size() int {
	c.mu.RLock()
	var size int
	for _, p := range c.pool {
		size += p.Size()
	}
	c.mu.RUnlock()
	return size
}

func (c *clientPool) conn(nodeID uint64) (net.Conn, error) {
	c.mu.RLock()
	p := c.pool[nodeID]
	c.mu.RUnlock()
	return p.Get()
}

func (c *clientPool) close() {
	c.mu.Lock()
	for _, p := range c.pool {
		p.Close()
	}
	c.mu.Unlock()
}
