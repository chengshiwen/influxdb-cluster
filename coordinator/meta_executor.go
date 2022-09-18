package coordinator

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"golang.org/x/sync/errgroup"
)

// MetaExecutor executes meta queries on one or more data nodes.
type MetaExecutor struct {
	timeout     time.Duration
	dialTimeout time.Duration

	nodeExecutor interface {
		executeOnNode(nodeID uint64, stmt influxql.Statement, database string) error
	}

	MetaClient interface {
		NodeID() uint64
		DataNode(id uint64) (ni *meta.NodeInfo, err error)
		DataNodes() []meta.NodeInfo
	}

	TLSConfig *tls.Config
}

// NewMetaExecutor returns a new initialized *MetaExecutor.
func NewMetaExecutor(timeout, dialTimeout time.Duration) *MetaExecutor {
	e := &MetaExecutor{
		timeout:     timeout,
		dialTimeout: dialTimeout,
	}
	e.nodeExecutor = e
	return e
}

// remoteNodeError wraps an error with context about a node that
// returned the error.
type remoteNodeError struct {
	id  uint64
	err error
}

func (e remoteNodeError) Error() string {
	return fmt.Sprintf("partial success, node %d may be down (%s)", e.id, e.err)
}

// ExecuteStatement executes a single InfluxQL statement on all nodes in the cluster concurrently.
func (e *MetaExecutor) ExecuteStatement(stmt influxql.Statement, database string) error {
	// Get a list of all nodes the query needs to be executed on.
	nodes := e.MetaClient.DataNodes()
	if len(nodes) < 1 {
		return nil
	}

	// Start a goroutine to execute the statement on each of the remote nodes.
	var wg sync.WaitGroup
	errs := make(chan error, len(nodes)-1)
	defer close(errs)
	localID := e.MetaClient.NodeID()
	for _, node := range nodes {
		if localID == node.ID {
			continue // Don't execute statement on ourselves.
		}

		wg.Add(1)
		go func(nodeID uint64) {
			defer wg.Done()
			if err := e.nodeExecutor.executeOnNode(nodeID, stmt, database); err != nil {
				errs <- remoteNodeError{id: nodeID, err: err}
			}
		}(node.ID)
	}

	// Wait on n-1 nodes to execute the statement and respond.
	wg.Wait()

	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

// executeOnNode executes a single InfluxQL statement on a single node.
func (e *MetaExecutor) executeOnNode(nodeID uint64, stmt influxql.Statement, database string) error {
	conn, err := e.dial(nodeID)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Build RPC request.
	var request ExecuteStatementRequest
	request.SetStatement(stmt.String())
	request.SetDatabase(database)

	// Write request.
	if err := EncodeTLV(conn, executeStatementRequestMessage, &request); err != nil {
		return err
	}

	// Read the response.
	var resp ExecuteStatementResponse
	if _, err := DecodeTLV(conn, &resp); err != nil {
		return err
	}

	if resp.Code() != 0 {
		return fmt.Errorf("error code %d: %s", resp.Code(), resp.Message())
	}
	return nil
}

// ExecuteQuery executes a single query on all nodes in the cluster concurrently.
func (e *MetaExecutor) ExecuteQuery(fn func() (interface{}, error), rfn func(nodeID uint64) (interface{}, error)) ([]interface{}, error) {
	nodes := e.MetaClient.DataNodes()
	localID := e.MetaClient.NodeID()

	var mu sync.Mutex
	var g errgroup.Group
	results := make([]interface{}, 0, len(nodes))

	g.Go(func() error {
		result, err := fn()
		if err != nil {
			return err
		}
		mu.Lock()
		results = append(results, result)
		mu.Unlock()
		return nil
	})

	for _, node := range nodes {
		nodeID := node.ID
		if nodeID != localID {
			g.Go(func() error {
				result, err := rfn(nodeID)
				if err != nil {
					return err
				}
				mu.Lock()
				results = append(results, result)
				mu.Unlock()
				return nil
			})
		}
	}

	err := g.Wait()
	return results, err
}

func (e *MetaExecutor) MeasurementNames(nodeID uint64, database string, cond influxql.Expr) ([][]byte, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLV(conn, measurementNamesRequestMessage, &MeasurementNamesRequest{
		Database:  database,
		Condition: cond,
	}); err != nil {
		return nil, err
	}

	// Read the response.
	var resp MeasurementNamesResponse
	if _, err := DecodeTLV(conn, &resp); err != nil {
		return nil, err
	}
	return resp.Names, nil
}

func (e *MetaExecutor) TagKeys(nodeID uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLV(conn, tagKeysRequestMessage, &TagKeysRequest{
		ShardIDs:  shardIDs,
		Condition: cond,
	}); err != nil {
		return nil, err
	}

	// Read the response.
	var resp TagKeysResponse
	if _, err := DecodeTLV(conn, &resp); err != nil {
		return nil, err
	}
	return resp.TagKeys, nil
}

func (e *MetaExecutor) TagValues(nodeID uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLV(conn, tagValuesRequestMessage, &TagValuesRequest{
		ShardIDs:  shardIDs,
		Condition: cond,
	}); err != nil {
		return nil, err
	}

	// Read the response.
	var resp TagValuesResponse
	if _, err := DecodeTLV(conn, &resp); err != nil {
		return nil, err
	}
	return resp.TagValues, nil
}

func (e *MetaExecutor) SeriesCardinality(nodeID uint64, database string) (int64, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLV(conn, seriesCardinalityRequestMessage, &SeriesCardinalityRequest{
		Database: database,
	}); err != nil {
		return 0, err
	}

	// Read the response.
	var resp SeriesCardinalityResponse
	if _, err := DecodeTLV(conn, &resp); err != nil {
		return 0, err
	}
	return resp.Cardinality, nil
}

func (e *MetaExecutor) MeasurementsCardinality(nodeID uint64, database string) (int64, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLV(conn, measurementsCardinalityRequestMessage, &MeasurementsCardinalityRequest{
		Database: database,
	}); err != nil {
		return 0, err
	}

	// Read the response.
	var resp MeasurementsCardinalityResponse
	if _, err := DecodeTLV(conn, &resp); err != nil {
		return 0, err
	}
	return resp.Cardinality, nil
}

func (e *MetaExecutor) FieldDimensions(nodeID uint64, shardIDs []uint64, m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLV(conn, fieldDimensionsRequestMessage, &FieldDimensionsRequest{
		ShardIDs:    shardIDs,
		Measurement: *m,
	}); err != nil {
		return nil, nil, err
	}

	// Read the response.
	var resp FieldDimensionsResponse
	if _, err := DecodeTLV(conn, &resp); err != nil {
		return nil, nil, err
	}
	return resp.Fields, resp.Dimensions, resp.Err
}

func (e *MetaExecutor) MapType(nodeID uint64, shardIDs []uint64, m *influxql.Measurement, field string) (influxql.DataType, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return influxql.Unknown, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLV(conn, mapTypeRequestMessage, &MapTypeRequest{
		ShardIDs:    shardIDs,
		Measurement: *m,
		Field:       field,
	}); err != nil {
		return influxql.Unknown, err
	}

	// Read the response.
	var resp MapTypeResponse
	if _, err := DecodeTLV(conn, &resp); err != nil {
		return influxql.Unknown, err
	}
	return resp.Type, nil
}

func (e *MetaExecutor) CreateIterator(nodeID uint64, shardIDs []uint64, ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, err
	}

	var resp CreateIteratorResponse
	if err := func() error {
		// Write request.
		if err := EncodeTLV(conn, createIteratorRequestMessage, &CreateIteratorRequest{
			ShardIDs:    shardIDs,
			Measurement: *m,
			Opt:         opt,
		}); err != nil {
			return err
		}

		// Read the response.
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != nil {
			return err
		}

		return nil
	}(); err != nil {
		conn.Close()
		return nil, err
	}

	return query.NewReaderIterator(ctx, conn, resp.Type, resp.Stats), nil
}

func (e *MetaExecutor) IteratorCost(nodeID uint64, shardIDs []uint64, m *influxql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return query.IteratorCost{}, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLV(conn, iteratorCostRequestMessage, &IteratorCostRequest{
		ShardIDs:    shardIDs,
		Measurement: *m,
		Opt:         opt,
	}); err != nil {
		return query.IteratorCost{}, err
	}

	// Read the response.
	var resp IteratorCostResponse
	if _, err := DecodeTLV(conn, &resp); err != nil {
		return query.IteratorCost{}, err
	}
	return resp.Cost, resp.Err
}

// dial returns a connection to a single node in the cluster.
func (e *MetaExecutor) dial(nodeID uint64) (net.Conn, error) {
	ni, err := e.MetaClient.DataNode(nodeID)
	if err != nil {
		return nil, err
	}

	conn, err := tcp.DialTLSTimeout("tcp", ni.TCPAddr, e.TLSConfig, e.dialTimeout)
	if err != nil {
		return nil, err
	}
	if e.timeout > 0 {
		conn.SetDeadline(time.Now().Add(e.timeout))
	}

	// Write the cluster multiplexing header byte
	if _, err := conn.Write([]byte{MuxHeader}); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}
