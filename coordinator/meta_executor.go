package coordinator

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"golang.org/x/sync/errgroup"
)

// MetaExecutor executes meta queries on one or more data nodes.
type MetaExecutor struct {
	pool        *clientPool
	timeout     time.Duration
	dialTimeout time.Duration
	idleTime    time.Duration
	maxStreams  int

	nodeExecutor interface {
		executeOnNode(nodeID uint64, stmt influxql.Statement, database string) error
	}

	MetaClient interface {
		NodeID() uint64
		DataNode(id uint64) (ni *meta.NodeInfo, err error)
		DataNodes() []meta.NodeInfo
		DataNodeByTCPAddr(tcpAddr string) (*meta.NodeInfo, error)
	}

	TLSConfig *tls.Config
}

// NewMetaExecutor returns a new initialized *MetaExecutor.
func NewMetaExecutor(timeout, dialTimeout, idleTime time.Duration, maxStreams int) *MetaExecutor {
	e := &MetaExecutor{
		pool:        newClientPool(),
		timeout:     timeout,
		dialTimeout: dialTimeout,
		idleTime:    idleTime,
		maxStreams:  maxStreams,
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
	if err := EncodeTLVT(conn, executeStatementRequestMessage, &request, e.timeout); err != nil {
		MarkUnusable(conn)
		return err
	}

	// Read the response.
	var resp ExecuteStatementResponse
	if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
		MarkUnusable(conn)
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

func (e *MetaExecutor) TaskManagerStatement(nodeID uint64, stmt influxql.Statement) (query.Result, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return query.Result{}, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLVT(conn, taskManagerStatementRequestMessage, &TaskManagerStatementRequest{
		Statement: stmt.String(),
	}, e.timeout); err != nil {
		MarkUnusable(conn)
		return query.Result{}, err
	}

	// Read the response.
	var resp TaskManagerStatementResponse
	if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
		MarkUnusable(conn)
		return query.Result{}, err
	}
	return resp.Result, resp.Err
}

func (e *MetaExecutor) MeasurementNames(nodeID uint64, database string, retentionPolicy string, cond influxql.Expr) ([][]byte, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLVT(conn, measurementNamesRequestMessage, &MeasurementNamesRequest{
		Database:        database,
		RetentionPolicy: retentionPolicy,
		Condition:       cond,
	}, e.timeout); err != nil {
		MarkUnusable(conn)
		return nil, err
	}

	// Read the response.
	var resp MeasurementNamesResponse
	if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
		MarkUnusable(conn)
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
	if err := EncodeTLVT(conn, tagKeysRequestMessage, &TagKeysRequest{
		ShardIDs:  shardIDs,
		Condition: cond,
	}, e.timeout); err != nil {
		MarkUnusable(conn)
		return nil, err
	}

	// Read the response.
	var resp TagKeysResponse
	if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
		MarkUnusable(conn)
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
	if err := EncodeTLVT(conn, tagValuesRequestMessage, &TagValuesRequest{
		ShardIDs:  shardIDs,
		Condition: cond,
	}, e.timeout); err != nil {
		MarkUnusable(conn)
		return nil, err
	}

	// Read the response.
	var resp TagValuesResponse
	if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
		MarkUnusable(conn)
		return nil, err
	}
	return resp.TagValues, nil
}

func (e *MetaExecutor) SeriesSketches(nodeID uint64, database string) (estimator.Sketch, estimator.Sketch, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLVT(conn, seriesSketchesRequestMessage, &SeriesSketchesRequest{
		Database: database,
	}, e.timeout); err != nil {
		MarkUnusable(conn)
		return nil, nil, err
	}

	// Read the response.
	var resp SeriesSketchesResponse
	if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
		MarkUnusable(conn)
		return nil, nil, err
	}
	return resp.Sketch, resp.TSSketch, nil
}

func (e *MetaExecutor) MeasurementsSketches(nodeID uint64, database string) (estimator.Sketch, estimator.Sketch, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLVT(conn, measurementsSketchesRequestMessage, &MeasurementsSketchesRequest{
		Database: database,
	}, e.timeout); err != nil {
		MarkUnusable(conn)
		return nil, nil, err
	}

	// Read the response.
	var resp MeasurementsSketchesResponse
	if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
		MarkUnusable(conn)
		return nil, nil, err
	}
	return resp.Sketch, resp.TSSketch, nil
}

func (e *MetaExecutor) FieldDimensions(nodeID uint64, shardIDs []uint64, m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLVT(conn, fieldDimensionsRequestMessage, &FieldDimensionsRequest{
		ShardIDs:    shardIDs,
		Measurement: *m,
	}, e.timeout); err != nil {
		MarkUnusable(conn)
		return nil, nil, err
	}

	// Read the response.
	var resp FieldDimensionsResponse
	if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
		MarkUnusable(conn)
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
	if err := EncodeTLVT(conn, mapTypeRequestMessage, &MapTypeRequest{
		ShardIDs:    shardIDs,
		Measurement: *m,
		Field:       field,
	}, e.timeout); err != nil {
		MarkUnusable(conn)
		return influxql.Unknown, err
	}

	// Read the response.
	var resp MapTypeResponse
	if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
		MarkUnusable(conn)
		return influxql.Unknown, err
	}
	return resp.Type, nil
}

func (e *MetaExecutor) CreateIterator(nodeID uint64, shardIDs []uint64, ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, err
	}
	MarkUnusable(conn)

	var sc tracing.SpanContext
	if span := tracing.SpanFromContext(ctx); span != nil {
		span = span.StartSpan("remote_iterator_request")
		defer span.Finish()

		span.SetLabels("node_id", strconv.Itoa(int(nodeID)))
		ctx = tracing.NewContextWithSpan(ctx, span)
		sc = span.Context()
	}

	var resp CreateIteratorResponse
	if err := func() error {
		// Write request.
		if err := EncodeTLVT(conn, createIteratorRequestMessage, &CreateIteratorRequest{
			ShardIDs:    shardIDs,
			Measurement: *m,
			Opt:         opt,
			SpanContext: sc,
		}, e.timeout); err != nil {
			return err
		}

		// Read the response.
		if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
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
	if err := EncodeTLVT(conn, iteratorCostRequestMessage, &IteratorCostRequest{
		ShardIDs:    shardIDs,
		Measurement: *m,
		Opt:         opt,
	}, e.timeout); err != nil {
		MarkUnusable(conn)
		return query.IteratorCost{}, err
	}

	// Read the response.
	var resp IteratorCostResponse
	if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
		MarkUnusable(conn)
		return query.IteratorCost{}, err
	}
	return resp.Cost, resp.Err
}

func (e *MetaExecutor) ReadFilter(nodeID uint64, shardIDs []uint64, ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, err
	}
	MarkUnusable(conn)

	if err := func() error {
		// Write request.
		if err := EncodeTLVT(conn, storeReadFilterRequestMessage, &StoreReadFilterRequest{
			ShardIDs: shardIDs,
			Request:  *req,
		}, e.timeout); err != nil {
			return err
		}

		// Read the response.
		var resp StoreReadFilterResponse
		if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
			return err
		} else if resp.Err != nil {
			return err
		}

		return nil
	}(); err != nil {
		conn.Close()
		return nil, err
	}

	return reads.NewResultSetStreamReader(NewStoreStreamReceiver(conn)), nil
}

func (e *MetaExecutor) ReadGroup(nodeID uint64, shardIDs []uint64, ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	conn, err := e.dial(nodeID)
	if err != nil {
		return nil, err
	}
	MarkUnusable(conn)

	if err := func() error {
		// Write request.
		if err := EncodeTLVT(conn, storeReadGroupRequestMessage, &StoreReadGroupRequest{
			ShardIDs: shardIDs,
			Request:  *req,
		}, e.timeout); err != nil {
			return err
		}

		// Read the response.
		var resp StoreReadGroupResponse
		if _, err := DecodeTLVT(conn, &resp, e.timeout); err != nil {
			return err
		} else if resp.Err != nil {
			return err
		}

		return nil
	}(); err != nil {
		conn.Close()
		return nil, err
	}

	return reads.NewGroupResultSetStreamReader(NewStoreStreamReceiver(conn)), nil
}

// dial returns a connection to a single node in the cluster.
func (e *MetaExecutor) dial(nodeID uint64) (net.Conn, error) {
	// If we don't have a connection pool for that addr yet, create one
	_, ok := e.pool.getPool(nodeID)
	if !ok {
		factory := &connFactory{nodeID: nodeID, clientPool: e.pool, timeout: e.dialTimeout, tlsConfig: e.TLSConfig}
		factory.metaClient = e.MetaClient

		p, err := NewBoundedPool(1, e.maxStreams, e.idleTime, factory.dial)
		if err != nil {
			return nil, err
		}
		e.pool.setPool(nodeID, p)
	}
	return e.pool.conn(nodeID)
}

// Close closes MetaExecutor's pool
func (e *MetaExecutor) Close() error {
	if e.pool == nil {
		return ErrClientClosed
	}
	e.pool.close()
	e.pool = nil
	return nil
}
