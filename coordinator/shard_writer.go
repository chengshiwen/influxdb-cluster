package coordinator

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tcp"
)

// ShardWriter writes a set of points to a shard.
type ShardWriter struct {
	pool        *clientPool
	timeout     time.Duration
	dialTimeout time.Duration
	idleTime    time.Duration
	maxStreams  int

	MetaClient interface {
		DataNode(id uint64) (ni *meta.NodeInfo, err error)
		ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo)
	}

	TLSConfig *tls.Config
}

// NewShardWriter returns a new instance of ShardWriter.
func NewShardWriter(timeout, dialTimeout, idleTime time.Duration, maxStreams int) *ShardWriter {
	return &ShardWriter{
		pool:        newClientPool(),
		timeout:     timeout,
		dialTimeout: dialTimeout,
		idleTime:    idleTime,
		maxStreams:  maxStreams,
	}
}

// WriteShard writes time series points to a shard
func (w *ShardWriter) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	pts := make([][]byte, 0, len(points))
	for _, p := range points {
		b, err := p.MarshalBinary()
		if err != nil {
			continue
		}
		pts = append(pts, b)
	}
	return w.WriteShardBinary(shardID, ownerID, pts)
}

// WriteShardBinary writes time series binary points to a shard
func (w *ShardWriter) WriteShardBinary(shardID, ownerID uint64, points [][]byte) error {
	conn, err := w.dial(ownerID)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Determine the location of this shard and whether it still exists
	db, rp, sgi := w.MetaClient.ShardOwner(shardID)
	if sgi == nil {
		// If we can't get the shard group for this shard, then we need to drop this request
		// as it is no longer valid.  This could happen if writes were queued via
		// hinted handoff and we're processing the queue after a shard group was deleted.
		return nil
	}

	// Build write request.
	var request WriteShardRequest
	request.SetShardID(shardID)
	request.SetDatabase(db)
	request.SetRetentionPolicy(rp)
	request.SetBinaryPoints(points)

	// Marshal into protocol buffers.
	buf, err := request.MarshalBinary()
	if err != nil {
		return err
	}

	// Write request.
	if err := WriteTLVT(conn, writeShardRequestMessage, buf, w.timeout); err != nil {
		MarkUnusable(conn)
		return err
	}

	// Read the response.
	_, buf, err = ReadTLVT(conn, w.timeout)
	if err != nil {
		MarkUnusable(conn)
		return err
	}

	// Unmarshal response.
	var response WriteShardResponse
	if err := response.UnmarshalBinary(buf); err != nil {
		return err
	}

	if response.Code() != 0 {
		return fmt.Errorf("error code %d: %s", response.Code(), response.Message())
	}

	return nil
}

// dial returns a connection to a single node in the cluster.
func (w *ShardWriter) dial(nodeID uint64) (net.Conn, error) {
	// If we don't have a connection pool for that addr yet, create one
	_, ok := w.pool.getPool(nodeID)
	if !ok {
		factory := &connFactory{nodeID: nodeID, clientPool: w.pool, timeout: w.dialTimeout, tlsConfig: w.TLSConfig}
		factory.metaClient = w.MetaClient

		p, err := NewBoundedPool(1, w.maxStreams, w.idleTime, factory.dial)
		if err != nil {
			return nil, err
		}
		w.pool.setPool(nodeID, p)
	}
	return w.pool.conn(nodeID)
}

// Close closes ShardWriter's pool
func (w *ShardWriter) Close() error {
	if w.pool == nil {
		return ErrClientClosed
	}
	w.pool.close()
	w.pool = nil
	return nil
}

const (
	maxConnections = 5000
	maxRetries     = 3
)

var errMaxConnectionsExceeded = fmt.Errorf("can not exceed max connections of %d", maxConnections)

type connFactory struct {
	nodeID  uint64
	timeout time.Duration

	clientPool interface {
		size() int
	}

	metaClient interface {
		DataNode(id uint64) (ni *meta.NodeInfo, err error)
	}

	tlsConfig *tls.Config
}

func (c *connFactory) dial() (net.Conn, error) {
	if c.clientPool.size() > maxConnections {
		return nil, errMaxConnectionsExceeded
	}

	ni, err := c.metaClient.DataNode(c.nodeID)
	if err != nil {
		return nil, err
	}

	if ni == nil {
		return nil, fmt.Errorf("node %d does not exist", c.nodeID)
	}

	conn, err := tcp.DialTLSTimeout("tcp", ni.TCPAddr, c.tlsConfig, c.timeout)
	if err != nil {
		return nil, err
	}

	// Write a marker byte for cluster messages.
	_, err = conn.Write([]byte{MuxHeader})
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}
