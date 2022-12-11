package coordinator

import (
	"bufio"
	"bytes"
	"context"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// MaxMessageSize defines how large a message can be before we reject it
const MaxMessageSize = 1024 * 1024 * 1024 // 1GB

// MuxHeader is the header byte used for the TCP muxer.
const MuxHeader = 2

// Statistics maintained by the coordinator package
const (
	statWriteShardReq       = "writeShardReq"
	statWriteShardPointsReq = "writeShardPointsReq"
	statWriteShardFail      = "writeShardFail"
	statCreateIteratorReq   = "createIteratorReq"
	statIteratorCostReq     = "iteratorCostReq"
	statFieldDimensionsReq  = "fieldDimensionsReq"
	statMapTypeReq          = "mapTypeReq"
	statExpandSourcesReq    = "expandSourcesReq"
	statBackupShardReq      = "backupShardReq"
	statCopyShardReq        = "copyShardReq"
	statRemoveShardReq      = "removeShardReq"
	statShowShardsReq       = "showShardsReq"
)

const (
	writeShardRequestMessage byte = iota + 1
	writeShardResponseMessage

	executeStatementRequestMessage
	executeStatementResponseMessage

	measurementNamesRequestMessage
	measurementNamesResponseMessage

	tagKeysRequestMessage
	tagKeysResponseMessage

	tagValuesRequestMessage
	tagValuesResponseMessage

	seriesCardinalityRequestMessage
	seriesCardinalityResponseMessage

	measurementsCardinalityRequestMessage
	measurementsCardinalityResponseMessage

	createIteratorRequestMessage
	createIteratorResponseMessage

	iteratorCostRequestMessage
	iteratorCostResponseMessage

	fieldDimensionsRequestMessage
	fieldDimensionsResponseMessage

	mapTypeRequestMessage
	mapTypeResponseMessage

	expandSourcesRequestMessage
	expandSourcesResponseMessage

	backupShardRequestMessage
	backupShardResponseMessage

	copyShardRequestMessage
	copyShardResponseMessage

	removeShardRequestMessage
	removeShardResponseMessage

	showShardsRequestMessage
	showShardsResponseMessage

	joinClusterRequestMessage
	joinClusterResponseMessage

	leaveClusterRequestMessage
	leaveClusterResponseMessage

	removeHintedHandoffRequestMessage
	removeHintedHandoffResponseMessage
)

// BackupTimeout is the time before a connection times out when performing a backup.
const BackupTimeout = 30 * time.Second

// Service processes data received over raw TCP connections.
type Service struct {
	mu sync.RWMutex

	config  Config
	wg      sync.WaitGroup
	closing chan struct{}

	Listener        net.Listener
	DefaultListener net.Listener
	httpListener    *chanListener // http channel-based listener

	Server interface {
		Reset() error
		HTTPAddr() string
		HTTPScheme() string
		TCPAddr() string
	}

	MetaClient interface {
		NodeID() uint64
		MetaServers() []string
		SetMetaServers(a []string)
		DataNode(id uint64) (*meta.NodeInfo, error)
		CreateDataNode(httpAddr, tcpAddr string) (*meta.NodeInfo, error)
		DataNodeByTCPAddr(tcpAddr string) (*meta.NodeInfo, error)
		Status() (*meta.MetaNodeStatus, error)
		Save() error
	}

	HintedHandoff interface {
		RemoveNode(ownerID uint64) error
	}

	TSDBStore TSDBStore
	Monitor   *monitor.Monitor

	Logger *zap.Logger
	stats  *Statistics
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	return &Service{
		config:  c,
		closing: make(chan struct{}),
		Logger:  zap.NewNop(),
		stats:   &Statistics{},
	}
}

// Open opens the network listener and begins serving requests.
func (s *Service) Open() error {
	s.Logger.Info("Starting coordinator service")

	// wait for the listeners to start
	timeout := time.Now().Add(time.Second)
	for {
		if s.Listener.Addr() != nil && s.DefaultListener.Addr() != nil {
			break
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("unable to open without coordinator listener running")
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.httpListener = newChanListener(s.DefaultListener.Addr())

	if !s.config.ClusterTracing {
		s.Logger = zap.NewNop()
	}

	// Begin serving connections.
	s.wg.Add(3)
	go s.serve()
	go s.serveDefault()
	go s.serveHTTP()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "coordinator"))
}

// Statistics maintains the statistics for the coordinator service.
type Statistics struct {
	WriteShardReq       int64
	WriteShardPointsReq int64
	WriteShardFail      int64
	CreateIteratorReq   int64
	IteratorCostReq     int64
	FieldDimensionsReq  int64
	MapTypeReq          int64
	ExpandSourcesReq    int64
	BackupShardReq      int64
	CopyShardReq        int64
	RemoveShardReq      int64
	ShowShardsReq       int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "coordinator",
		Tags: tags,
		Values: map[string]interface{}{
			statWriteShardReq:       atomic.LoadInt64(&s.stats.WriteShardReq),
			statWriteShardPointsReq: atomic.LoadInt64(&s.stats.WriteShardPointsReq),
			statWriteShardFail:      atomic.LoadInt64(&s.stats.WriteShardFail),
			statCreateIteratorReq:   atomic.LoadInt64(&s.stats.CreateIteratorReq),
			statIteratorCostReq:     atomic.LoadInt64(&s.stats.IteratorCostReq),
			statFieldDimensionsReq:  atomic.LoadInt64(&s.stats.FieldDimensionsReq),
			statMapTypeReq:          atomic.LoadInt64(&s.stats.MapTypeReq),
			statExpandSourcesReq:    atomic.LoadInt64(&s.stats.ExpandSourcesReq),
			statBackupShardReq:      atomic.LoadInt64(&s.stats.BackupShardReq),
			statCopyShardReq:        atomic.LoadInt64(&s.stats.CopyShardReq),
			statRemoveShardReq:      atomic.LoadInt64(&s.stats.RemoveShardReq),
			statShowShardsReq:       atomic.LoadInt64(&s.stats.ShowShardsReq),
		},
	}}
}

// serve accepts connections from the listener and handles them.
func (s *Service) serve() {
	defer s.wg.Done()

	for {
		// Check if the service is shutting down.
		select {
		case <-s.closing:
			return
		default:
		}

		// Accept the next connection.
		conn, err := s.Listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "connection closed") {
				s.Logger.Info("Coordinator listener closed")
				return
			}
			s.Logger.Error("Accept error", zap.Error(err))
			continue
		}

		// Delegate connection handling to a separate goroutine.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

// Close shuts down the listener and waits for all connections to finish.
func (s *Service) Close() error {
	if s.Listener != nil {
		s.Listener.Close()
	}
	if s.DefaultListener != nil {
		s.DefaultListener.Close()
	}
	if err := s.httpListener.Close(); err != nil {
		return err
	}

	// Shut down all handlers.
	close(s.closing)
	s.wg.Wait()

	return nil
}

// handleConn services an individual TCP connection.
func (s *Service) handleConn(conn net.Conn) {
	// Ensure connection is closed when service is closed.
	closing := make(chan struct{})
	defer close(closing)
	go func() {
		select {
		case <-closing:
		case <-s.closing:
		}
		conn.Close()
	}()

	s.Logger.Info("Accept remote connection", zap.String("addr", conn.RemoteAddr().String()))
	defer func() {
		s.Logger.Info("Close remote connection", zap.String("addr", conn.RemoteAddr().String()))
	}()
	for {
		// Read type-length-value.
		typ, err := ReadType(conn)
		if err != nil {
			if strings.HasSuffix(err.Error(), "EOF") || strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			s.Logger.Error("Unable to read type", zap.Error(err))
			return
		}

		// Delegate message processing by type.
		switch typ {
		case writeShardRequestMessage:
			buf, err := ReadLV(conn)
			if err != nil {
				s.Logger.Error("Unable to read length-value", zap.Error(err))
				return
			}
			atomic.AddInt64(&s.stats.WriteShardReq, 1)
			err = s.processWriteShardRequest(buf)
			if err != nil {
				s.Logger.Error("Process write shard error", zap.Error(err))
			}
			s.writeShardResponse(conn, err)
		case executeStatementRequestMessage:
			buf, err := ReadLV(conn)
			if err != nil {
				s.Logger.Error("Unable to read length-value", zap.Error(err))
				return
			}
			err = s.processExecuteStatementRequest(buf)
			if err != nil {
				s.Logger.Error("Process execute statement error", zap.Error(err))
			}
			s.executeStatementResponse(conn, err)
		case measurementNamesRequestMessage:
			s.processMeasurementNamesRequest(conn)
			return
		case tagKeysRequestMessage:
			s.processTagKeysRequest(conn)
			return
		case tagValuesRequestMessage:
			s.processTagValuesRequest(conn)
			return
		case seriesCardinalityRequestMessage:
			s.processSeriesCardinalityRequest(conn)
			return
		case measurementsCardinalityRequestMessage:
			s.processMeasurementsCardinalityRequest(conn)
			return
		case createIteratorRequestMessage:
			atomic.AddInt64(&s.stats.CreateIteratorReq, 1)
			s.processCreateIteratorRequest(conn)
			return
		case iteratorCostRequestMessage:
			atomic.AddInt64(&s.stats.IteratorCostReq, 1)
			s.processIteratorCostRequest(conn)
			return
		case fieldDimensionsRequestMessage:
			atomic.AddInt64(&s.stats.FieldDimensionsReq, 1)
			s.processFieldDimensionsRequest(conn)
			return
		case mapTypeRequestMessage:
			atomic.AddInt64(&s.stats.MapTypeReq, 1)
			s.processMapTypeRequest(conn)
			return
		case expandSourcesRequestMessage:
			atomic.AddInt64(&s.stats.ExpandSourcesReq, 1)
			s.processExpandSourcesRequest(conn)
			return
		case backupShardRequestMessage:
			atomic.AddInt64(&s.stats.BackupShardReq, 1)
			s.processBackupShardRequest(conn)
			return
		case copyShardRequestMessage:
			atomic.AddInt64(&s.stats.CopyShardReq, 1)
			s.processCopyShardRequest(conn)
			return
		case removeShardRequestMessage:
			atomic.AddInt64(&s.stats.RemoveShardReq, 1)
			s.processRemoveShardRequest(conn)
			return
		case showShardsRequestMessage:
			atomic.AddInt64(&s.stats.ShowShardsReq, 1)
			s.processShowShardsRequest(conn)
			return
		case joinClusterRequestMessage:
			s.processJoinClusterRequest(conn)
			return
		case leaveClusterRequestMessage:
			s.processLeaveClusterRequest(conn)
			return
		case removeHintedHandoffRequestMessage:
			s.processRemoveHintedHandoffRequest(conn)
			return
		default:
			s.Logger.Warn("Coordinator service message type not found", zap.Uint8("Type", typ))
		}
	}
}

func (s *Service) processExecuteStatementRequest(buf []byte) error {
	// Unmarshal the request.
	var req ExecuteStatementRequest
	if err := req.UnmarshalBinary(buf); err != nil {
		return err
	}

	// Parse the InfluxQL statement.
	stmt, err := influxql.ParseStatement(req.Statement())
	if err != nil {
		return err
	}

	return s.executeStatement(stmt, req.Database())
}

func (s *Service) executeStatement(stmt influxql.Statement, database string) error {
	switch t := stmt.(type) {
	case *influxql.DeleteSeriesStatement:
		return s.TSDBStore.DeleteSeries(database, t.Sources, t.Condition)
	case *influxql.DropDatabaseStatement:
		return s.TSDBStore.DeleteDatabase(t.Name)
	case *influxql.DropMeasurementStatement:
		return s.TSDBStore.DeleteMeasurement(database, t.Name)
	case *influxql.DropSeriesStatement:
		return s.TSDBStore.DeleteSeries(database, t.Sources, t.Condition)
	case *influxql.DropShardStatement:
		return s.TSDBStore.DeleteShard(t.ID)
	case *influxql.DropRetentionPolicyStatement:
		return s.TSDBStore.DeleteRetentionPolicy(database, t.Name)
	default:
		return fmt.Errorf("%q should not be executed across a cluster", stmt.String())
	}
}

func (s *Service) executeStatementResponse(w io.Writer, e error) {
	// Build response.
	var resp ExecuteStatementResponse
	if e != nil {
		resp.SetCode(1)
		resp.SetMessage(e.Error())
	} else {
		resp.SetCode(0)
	}

	// Marshal response to binary.
	buf, err := resp.MarshalBinary()
	if err != nil {
		s.Logger.Error("Error marshalling ExecuteStatement response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := WriteTLV(w, executeStatementResponseMessage, buf); err != nil {
		s.Logger.Error("Error writing ExecuteStatement response", zap.Error(err))
	}
}

func (s *Service) processWriteShardRequest(buf []byte) error {
	// Build request
	var req WriteShardRequest
	if err := req.UnmarshalBinary(buf); err != nil {
		return err
	}

	points := req.Points()
	atomic.AddInt64(&s.stats.WriteShardPointsReq, int64(len(points)))
	err := s.TSDBStore.WriteToShard(req.ShardID(), points)

	// We may have received a write for a shard that we don't have locally because the
	// sending node may have just created the shard (via the metastore) and the write
	// arrived before the local store could create the shard.  In this case, we need
	// to check the metastore to determine what database and retention policy this
	// shard should reside within.
	if err == tsdb.ErrShardNotFound {
		db, rp := req.Database(), req.RetentionPolicy()
		if db == "" || rp == "" {
			s.Logger.Warn("Drop write request: no database or retention policy received", zap.Uint64("shard", req.ShardID()))
			return nil
		}

		err = s.TSDBStore.CreateShard(req.Database(), req.RetentionPolicy(), req.ShardID(), true)
		if err != nil {
			atomic.AddInt64(&s.stats.WriteShardFail, 1)
			return fmt.Errorf("create shard %d: %s", req.ShardID(), err)
		}

		err = s.TSDBStore.WriteToShard(req.ShardID(), points)
		if err != nil {
			atomic.AddInt64(&s.stats.WriteShardFail, 1)
			return fmt.Errorf("write shard %d: %s", req.ShardID(), err)
		}
	}

	if err != nil {
		atomic.AddInt64(&s.stats.WriteShardFail, 1)
		return fmt.Errorf("write shard %d: %s", req.ShardID(), err)
	}

	return nil
}

func (s *Service) writeShardResponse(w io.Writer, e error) {
	// Build response.
	var resp WriteShardResponse
	if e != nil {
		resp.SetCode(1)
		resp.SetMessage(e.Error())
	} else {
		resp.SetCode(0)
	}

	// Marshal response to binary.
	buf, err := resp.MarshalBinary()
	if err != nil {
		s.Logger.Error("Error marshalling WriteShard response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := WriteTLV(w, writeShardResponseMessage, buf); err != nil {
		s.Logger.Error("Error writing WriteShard response", zap.Error(err))
	}
}

func (s *Service) processMeasurementNamesRequest(conn net.Conn) {
	names, err := func() ([][]byte, error) {
		// Parse request.
		var req MeasurementNamesRequest
		if err := DecodeLV(conn, &req); err != nil {
			return nil, err
		}
		// Return measurement names.
		return s.TSDBStore.MeasurementNames(context.Background(), nil, req.Database, req.Condition)
	}()
	if err != nil {
		s.Logger.Error("Error reading MeasurementNames request", zap.Error(err))
		EncodeTLV(conn, measurementNamesResponseMessage, &MeasurementNamesResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, measurementNamesResponseMessage, &MeasurementNamesResponse{
		Names: names,
	}); err != nil {
		s.Logger.Error("Error writing MeasurementNames response", zap.Error(err))
		return
	}
}

func (s *Service) processTagKeysRequest(conn net.Conn) {
	tagKeys, err := func() ([]tsdb.TagKeys, error) {
		// Parse request.
		var req TagKeysRequest
		if err := DecodeLV(conn, &req); err != nil {
			return nil, err
		}
		// Return tag keys.
		return s.TSDBStore.TagKeys(context.Background(), nil, req.ShardIDs, req.Condition)
	}()
	if err != nil {
		s.Logger.Error("Error reading TagKeys request", zap.Error(err))
		EncodeTLV(conn, tagKeysResponseMessage, &TagKeysResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, tagKeysResponseMessage, &TagKeysResponse{
		TagKeys: tagKeys,
	}); err != nil {
		s.Logger.Error("Error writing TagKeys response", zap.Error(err))
		return
	}
}

func (s *Service) processTagValuesRequest(conn net.Conn) {
	tagValues, err := func() ([]tsdb.TagValues, error) {
		// Parse request.
		var req TagValuesRequest
		if err := DecodeLV(conn, &req); err != nil {
			return nil, err
		}
		// Return tag values.
		return s.TSDBStore.TagValues(context.Background(), nil, req.ShardIDs, req.Condition)
	}()
	if err != nil {
		s.Logger.Error("Error reading TagValues request", zap.Error(err))
		EncodeTLV(conn, tagValuesResponseMessage, &TagValuesResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, tagValuesResponseMessage, &TagValuesResponse{
		TagValues: tagValues,
	}); err != nil {
		s.Logger.Error("Error writing TagValues response", zap.Error(err))
		return
	}
}

func (s *Service) processSeriesCardinalityRequest(conn net.Conn) {
	cardinality, err := func() (int64, error) {
		// Parse request.
		var req SeriesCardinalityRequest
		if err := DecodeLV(conn, &req); err != nil {
			return 0, err
		}
		// Return series cardinality.
		return s.TSDBStore.SeriesCardinality(context.Background(), req.Database)
	}()
	if err != nil {
		s.Logger.Error("Error reading SeriesCardinality request", zap.Error(err))
		EncodeTLV(conn, seriesCardinalityResponseMessage, &SeriesCardinalityResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, seriesCardinalityResponseMessage, &SeriesCardinalityResponse{
		Cardinality: cardinality,
	}); err != nil {
		s.Logger.Error("Error writing SeriesCardinality response", zap.Error(err))
		return
	}
}

func (s *Service) processMeasurementsCardinalityRequest(conn net.Conn) {
	cardinality, err := func() (int64, error) {
		// Parse request.
		var req MeasurementsCardinalityRequest
		if err := DecodeLV(conn, &req); err != nil {
			return 0, err
		}
		// Return measurements cardinality.
		return s.TSDBStore.MeasurementsCardinality(context.Background(), req.Database)
	}()
	if err != nil {
		s.Logger.Error("Error reading MeasurementsCardinality request", zap.Error(err))
		EncodeTLV(conn, measurementsCardinalityResponseMessage, &MeasurementsCardinalityResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, measurementsCardinalityResponseMessage, &MeasurementsCardinalityResponse{
		Cardinality: cardinality,
	}); err != nil {
		s.Logger.Error("Error writing MeasurementsCardinality response", zap.Error(err))
		return
	}
}

func (s *Service) processCreateIteratorRequest(conn net.Conn) {
	itr, err := func() (query.Iterator, error) {
		// Parse request.
		var req CreateIteratorRequest
		if err := DecodeLV(conn, &req); err != nil {
			return nil, err
		}

		// Collect a shard group with a list of shards for each shard.
		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if sg == nil {
			return nil, nil
		}

		// Generate a single iterator from all shards.
		ctx := context.Background()
		m := &req.Measurement
		if m.Regex != nil {
			measurements := sg.MeasurementsByRegex(m.Regex.Val)
			inputs := make([]query.Iterator, 0, len(measurements))
			if err := func() error {
				// Create a Measurement for each returned matching measurement value
				// from the regex.
				for _, measurement := range measurements {
					mm := m.Clone()
					mm.Name = measurement // Set the name to this matching regex value.
					input, err := sg.CreateIterator(ctx, mm, req.Opt)
					if err != nil {
						return err
					}
					if input != nil {
						inputs = append(inputs, input)
					}
				}
				return nil
			}(); err != nil {
				query.Iterators(inputs).Close()
				return nil, err
			}

			return query.Iterators(inputs).Merge(req.Opt)
		}
		return sg.CreateIterator(ctx, m, req.Opt)
	}()
	defer func() {
		if itr != nil {
			itr.Close()
		}
	}()
	if err != nil {
		s.Logger.Error("Error reading CreateIterator request", zap.Error(err))
		EncodeTLV(conn, createIteratorResponseMessage, &CreateIteratorResponse{Err: err})
		return
	}

	resp := CreateIteratorResponse{}
	if itr != nil {
		switch itr.(type) {
		case query.FloatIterator:
			resp.Type = influxql.Float
		case query.IntegerIterator:
			resp.Type = influxql.Integer
		case query.StringIterator:
			resp.Type = influxql.String
		case query.BooleanIterator:
			resp.Type = influxql.Boolean
		}
		resp.Stats = itr.Stats()
	}

	// Encode success response.
	if err := EncodeTLV(conn, createIteratorResponseMessage, &resp); err != nil {
		s.Logger.Error("Error writing CreateIterator response", zap.Error(err))
		return
	}

	// Exit if no iterator was produced.
	if itr == nil {
		return
	}

	// Stream iterator to connection.
	if err := query.NewIteratorEncoder(conn).EncodeIterator(itr); err != nil {
		s.Logger.Error("Error encoding CreateIterator iterator", zap.Error(err))
		return
	}
}

func (s *Service) processIteratorCostRequest(conn net.Conn) {
	cost, err := func() (query.IteratorCost, error) {
		// Parse request.
		var req IteratorCostRequest
		if err := DecodeLV(conn, &req); err != nil {
			return query.IteratorCost{}, err
		}

		// Collect a shard group with a list of shards for each shard.
		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if sg == nil {
			return query.IteratorCost{}, nil
		}

		// Calculate iterator cost from all shards.
		m := &req.Measurement
		if m.Regex != nil {
			var costs query.IteratorCost
			measurements := sg.MeasurementsByRegex(m.Regex.Val)
			for _, measurement := range measurements {
				cost, err := sg.IteratorCost(measurement, req.Opt)
				if err != nil {
					return query.IteratorCost{}, err
				}
				costs = costs.Combine(cost)
			}
			return costs, nil
		}
		return sg.IteratorCost(m.Name, req.Opt)
	}()
	if err != nil {
		s.Logger.Error("Error reading IteratorCost request", zap.Error(err))
		EncodeTLV(conn, iteratorCostResponseMessage, &IteratorCostResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, iteratorCostResponseMessage, &IteratorCostResponse{
		Cost: cost,
	}); err != nil {
		s.Logger.Error("Error writing IteratorCost response", zap.Error(err))
		return
	}
}

func (s *Service) processFieldDimensionsRequest(conn net.Conn) {
	fields, dimensions, err := func() (map[string]influxql.DataType, map[string]struct{}, error) {
		// Parse request.
		var req FieldDimensionsRequest
		if err := DecodeLV(conn, &req); err != nil {
			return nil, nil, err
		}

		// Collect a shard group with a list of shards for each shard.
		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if sg == nil {
			return nil, nil, nil
		}

		fields := make(map[string]influxql.DataType)
		dimensions := make(map[string]struct{})

		// Calculate fields, dimensions from all shards.
		m := &req.Measurement
		var measurements []string
		if m.Regex != nil {
			measurements = sg.MeasurementsByRegex(m.Regex.Val)
		} else {
			measurements = []string{m.Name}
		}

		f, d, err := sg.FieldDimensions(measurements)
		if err != nil {
			return nil, nil, err
		}
		for k, typ := range f {
			fields[k] = typ
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
		return fields, dimensions, nil
	}()
	if err != nil {
		s.Logger.Error("Error reading FieldDimensions request", zap.Error(err))
		EncodeTLV(conn, fieldDimensionsResponseMessage, &FieldDimensionsResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, fieldDimensionsResponseMessage, &FieldDimensionsResponse{
		Fields:     fields,
		Dimensions: dimensions,
	}); err != nil {
		s.Logger.Error("Error writing FieldDimensions response", zap.Error(err))
		return
	}
}

func (s *Service) processMapTypeRequest(conn net.Conn) {
	typ, err := func() (influxql.DataType, error) {
		// Parse request.
		var req MapTypeRequest
		if err := DecodeLV(conn, &req); err != nil {
			return influxql.Unknown, err
		}

		// Collect a shard group with a list of shards for each shard.
		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if sg == nil {
			return influxql.Unknown, nil
		}

		// Calculate data type from all shards.
		m := &req.Measurement
		var names []string
		if m.Regex != nil {
			names = sg.MeasurementsByRegex(m.Regex.Val)
		} else {
			names = []string{m.Name}
		}

		var typ influxql.DataType
		for _, name := range names {
			if m.SystemIterator != "" {
				name = m.SystemIterator
			}
			t := sg.MapType(name, req.Field)
			if typ.LessThan(t) {
				typ = t
			}
		}
		return typ, nil
	}()
	if err != nil {
		s.Logger.Error("Error reading MapType request", zap.Error(err))
		EncodeTLV(conn, mapTypeResponseMessage, &MapTypeResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, mapTypeResponseMessage, &MapTypeResponse{
		Type: typ,
	}); err != nil {
		s.Logger.Error("Error writing MapType response", zap.Error(err))
		return
	}
}

func (s *Service) processExpandSourcesRequest(conn net.Conn) {
	var sources influxql.Sources
	if err := func() error {
		// Parse request.
		var req ExpandSourcesRequest
		if err := DecodeLV(conn, &req); err != nil {
			return err
		}

		// Collect a shard group with a list of shards for each shard.
		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if sg == nil {
			return nil
		}

		// Expand sources from all shards.
		a, err := sg.ExpandSources(req.Sources)
		if err != nil {
			return err
		}
		sources = a

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading ExpandSources request", zap.Error(err))
		EncodeTLV(conn, expandSourcesResponseMessage, &ExpandSourcesResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, expandSourcesResponseMessage, &ExpandSourcesResponse{
		Sources: sources,
	}); err != nil {
		s.Logger.Error("Error writing ExpandSources response", zap.Error(err))
		return
	}
}

func (s *Service) processBackupShardRequest(conn net.Conn) {
	if err := func() error {
		// Parse request.
		var req BackupShardRequest
		if err := DecodeLV(conn, &req); err != nil {
			return err
		}

		// Backup from local shard to the connection.
		if err := s.TSDBStore.BackupShard(req.ShardID, req.Since, conn); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		s.Logger.Error("Error processing BackupShard request", zap.Error(err))
		return
	}
}

func (s *Service) processCopyShardRequest(conn net.Conn) {
	if err := func() error {
		// Parse request.
		var req CopyShardRequest
		if err := DecodeLV(conn, &req); err != nil {
			return err
		}

		// Begin streaming backup from remote server.
		r, err := s.backupRemoteShard(req.Host, req.ShardID, req.Since)
		if err != nil {
			return err
		}
		defer r.Close()

		// Create shard if it doesn't exist.
		if err := s.TSDBStore.CreateShard(req.Database, req.Policy, req.ShardID, true); err != nil {
			return err
		}

		// Restore to local shard.
		if err := s.TSDBStore.RestoreShard(req.ShardID, r); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading CopyShard request", zap.Error(err))
		EncodeTLV(conn, copyShardResponseMessage, &CopyShardResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, copyShardResponseMessage, &CopyShardResponse{}); err != nil {
		s.Logger.Error("Error writing CopyShard response", zap.Error(err))
		return
	}
}

// backupRemoteShard connects to a coordinator service on a remote host and streams a shard.
func (s *Service) backupRemoteShard(host string, shardID uint64, since time.Time) (io.ReadCloser, error) {
	tlsConfig := s.config.TLSClientConfig()
	conn, err := tcp.DialTLS("tcp", host, tlsConfig)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(BackupTimeout))

	if err := func() error {
		// Write the coordinator multiplexing header byte
		if _, err := conn.Write([]byte{MuxHeader}); err != nil {
			return err
		}

		// Write backup request.
		if err := EncodeTLV(conn, backupShardRequestMessage, &BackupShardRequest{
			ShardID: shardID,
			Since:   since,
		}); err != nil {
			return fmt.Errorf("error writing BackupShard request: %s", err)
		}

		return nil
	}(); err != nil {
		conn.Close()
		return nil, err
	}

	// Return the connection which will stream the rest of the backup.
	return conn, nil
}

func (s *Service) processRemoveShardRequest(conn net.Conn) {
	if err := func() error {
		// Parse request.
		var req RemoveShardRequest
		if err := DecodeLV(conn, &req); err != nil {
			return err
		}

		// Remove local shard.
		if err := s.TSDBStore.DeleteShard(req.ShardID); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading RemoveShard request", zap.Error(err))
		EncodeTLV(conn, removeShardResponseMessage, &RemoveShardResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, removeShardResponseMessage, &RemoveShardResponse{}); err != nil {
		s.Logger.Error("Error writing RemoveShard response", zap.Error(err))
		return
	}
}

func (s *Service) processShowShardsRequest(conn net.Conn) {
	shards := make(map[uint64]*meta.ShardOwnerInfo)
	if err := func() error {
		for _, id := range s.TSDBStore.ShardIDs() {
			owner := &meta.ShardOwnerInfo{
				ID:      s.MetaClient.NodeID(),
				TCPAddr: s.Server.TCPAddr(),
			}
			sh := s.TSDBStore.Shard(id)
			if sh != nil {
				owner.State = "hot"
				if isIdle, _ := sh.IsIdle(); isIdle {
					owner.State = "cold"
				}
				owner.LastModified = sh.LastModified()
				if size, err := sh.DiskSize(); err != nil {
					owner.Err = err.Error()
				} else {
					owner.Size = size
				}
			} else {
				owner.Err = "not found"
			}
			shards[id] = owner
		}

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading ShowShards request", zap.Error(err))
		EncodeTLV(conn, showShardsResponseMessage, &ShowShardsResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, showShardsResponseMessage, &ShowShardsResponse{Shards: shards}); err != nil {
		s.Logger.Error("Error writing ShowShards response", zap.Error(err))
		return
	}
}

func (s *Service) processJoinClusterRequest(conn net.Conn) {
	var node *meta.NodeInfo
	if err := func() error {
		// Parse request.
		var req JoinClusterRequest
		if err := DecodeLV(conn, &req); err != nil {
			return err
		}

		if len(req.MetaServers) == 0 {
			return errors.New("empty meta servers")
		}

		existedMetaServers := s.MetaClient.MetaServers()
		if len(existedMetaServers) > 0 {
			set := make(map[string]struct{})
			for _, ms := range existedMetaServers {
				set[ms] = struct{}{}
			}
			intersected := false
			for _, ms := range req.MetaServers {
				if _, ok := set[ms]; ok {
					intersected = true
					break
				}
			}
			if !intersected {
				return errors.New("already joined to cluster")
			}
		}

		s.MetaClient.SetMetaServers(req.MetaServers)

		ns, err := s.MetaClient.Status()
		if err != nil {
			return err
		}
		if ns.Leader == "" {
			return fmt.Errorf("no leader in meta servers: %v", req.MetaServers)
		}

		if req.Update {
			// Only check whether the current tcp addr already exists
			timeout := time.Now().Add(10 * time.Second)
			for {
				node, err = s.MetaClient.DataNodeByTCPAddr(s.Server.TCPAddr())
				if err == nil {
					break
				}
				if time.Now().After(timeout) {
					return fmt.Errorf("unable to update data node, timed out: %s", err)
				}
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			// If we've already created a data node for our id, we're done
			if node, err = s.MetaClient.DataNode(s.MetaClient.NodeID()); err == nil {
				return nil
			}

			timeout := time.Now().Add(10 * time.Second)
			for {
				node, err = s.MetaClient.CreateDataNode(s.Server.HTTPAddr(), s.Server.TCPAddr())
				if err == nil {
					break
				}
				if time.Now().After(timeout) {
					return fmt.Errorf("unable to create data node, timed out: %s", err)
				}
				time.Sleep(time.Second)
			}
		}

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading JoinCluster request", zap.Error(err))
		EncodeTLV(conn, joinClusterResponseMessage, &JoinClusterResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, joinClusterResponseMessage, &JoinClusterResponse{Node: node}); err != nil {
		s.Logger.Error("Error writing JoinCluster response", zap.Error(err))
		return
	}
}

func (s *Service) processLeaveClusterRequest(conn net.Conn) {
	if err := func() error {
		timeout := time.Now().Add(10 * time.Second)
		for {
			_, err := s.MetaClient.DataNodeByTCPAddr(s.Server.TCPAddr())
			if err != nil {
				break
			}
			if time.Now().After(timeout) {
				s.Logger.Warn(fmt.Sprintf("Data node %s still exists, already tried to check leaved and timed out", s.Server.TCPAddr()))
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		s.MetaClient.SetMetaServers(nil)
		if err := s.MetaClient.Save(); err != nil {
			s.Logger.Error("Error saving meta servers", zap.Error(err))
		}

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading LeaveCluster request", zap.Error(err))
		EncodeTLV(conn, leaveClusterResponseMessage, &LeaveClusterResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, leaveClusterResponseMessage, &LeaveClusterResponse{}); err != nil {
		s.Logger.Error("Error writing LeaveCluster response", zap.Error(err))
		return
	}

	// Reset server.
	go func() {
		if err := s.Server.Reset(); err != nil {
			s.Logger.Error("Error resetting data server", zap.Error(err))
		}
	}()
}

func (s *Service) processRemoveHintedHandoffRequest(conn net.Conn) {
	if err := func() error {
		// Parse request.
		var req RemoveHintedHandoffRequest
		if err := DecodeLV(conn, &req); err != nil {
			return err
		}

		// Remove local hinted handoff node.
		return s.HintedHandoff.RemoveNode(req.NodeID)
	}(); err != nil {
		s.Logger.Error("Error reading RemoveHintedHandoff request", zap.Error(err))
		EncodeTLV(conn, removeHintedHandoffResponseMessage, &RemoveHintedHandoffResponse{Err: err})
		return
	}

	// Encode success response.
	if err := EncodeTLV(conn, removeHintedHandoffResponseMessage, &RemoveHintedHandoffResponse{}); err != nil {
		s.Logger.Error("Error writing RemoveHintedHandoff response", zap.Error(err))
		return
	}
}

// serveDefault accepts connections from the default listener and handles them.
func (s *Service) serveDefault() {
	defer s.wg.Done()

	for {
		// Check if the service is shutting down.
		select {
		case <-s.closing:
			return
		default:
		}

		// Accept the next connection.
		conn, err := s.DefaultListener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "connection closed") {
				s.Logger.Info("Coordinator default listener closed")
				return
			}
			s.Logger.Error("Accept default error", zap.Error(err))
			continue
		}

		// Delegate connection handling to a separate goroutine.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleDefaultConn(conn)
		}()
	}
}

// handleDefaultConn services an individual TCP connection.
func (s *Service) handleDefaultConn(conn net.Conn) {
	// Read header into buffer to check if it's HTTP.
	var buf bytes.Buffer
	r := bufio.NewReader(io.TeeReader(conn, &buf))

	// Attempt to parse connection as HTTP.
	_, err := http.ReadRequest(r)

	// Rebuild connection from buffer and remaining connection data.
	bufr := bufio.NewReader(io.MultiReader(&buf, conn))
	conn = &readerConn{Conn: conn, r: bufr}

	// If no HTTP parsing error occurred then process as HTTP.
	if err == nil {
		s.httpListener.ch <- conn
		return
	}

	// Otherwise discard
	conn.Close()
}

// serveHTTP handles connections in HTTP format.
func (s *Service) serveHTTP() {
	defer s.wg.Done()
	h := newHandler(s)
	srv := &http.Server{Handler: h}
	srv.Serve(s.httpListener)
}

// ReadTLV reads a type-length-value record from r.
func ReadTLV(r io.Reader) (byte, []byte, error) {
	typ, err := ReadType(r)
	if err != nil {
		return 0, nil, err
	}

	buf, err := ReadLV(r)
	if err != nil {
		return 0, nil, err
	}
	return typ, buf, err
}

// ReadType reads the type from a TLV record.
func ReadType(r io.Reader) (byte, error) {
	var typ [1]byte
	if _, err := io.ReadFull(r, typ[:]); err != nil {
		return 0, fmt.Errorf("read message type: %s", err)
	}
	return typ[0], nil
}

// ReadLV reads the length-value from a TLV record.
func ReadLV(r io.Reader) ([]byte, error) {
	// Read the size of the message.
	var sz int64
	if err := binary.Read(r, binary.BigEndian, &sz); err != nil {
		return nil, fmt.Errorf("read message size: %s", err)
	}

	if sz >= MaxMessageSize {
		return nil, fmt.Errorf("max message size of %d exceeded: %d", MaxMessageSize, sz)
	}

	// Read the value.
	buf := make([]byte, sz)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read message value: %s", err)
	}

	return buf, nil
}

// WriteTLV writes a type-length-value record to w.
func WriteTLV(w io.Writer, typ byte, buf []byte) error {
	if err := WriteType(w, typ); err != nil {
		return err
	}
	if err := WriteLV(w, buf); err != nil {
		return err
	}
	return nil
}

// WriteType writes the type in a TLV record to w.
func WriteType(w io.Writer, typ byte) error {
	if _, err := w.Write([]byte{typ}); err != nil {
		return fmt.Errorf("write message type: %s", err)
	}
	return nil
}

// WriteLV writes the length-value in a TLV record to w.
func WriteLV(w io.Writer, buf []byte) error {
	// Write the size of the message.
	if err := binary.Write(w, binary.BigEndian, int64(len(buf))); err != nil {
		return fmt.Errorf("write message size: %s", err)
	}

	// Write the value.
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write message value: %s", err)
	}
	return nil
}

// EncodeTLV encodes v to a binary format and writes the record-length-value record to w.
func EncodeTLV(w io.Writer, typ byte, v encoding.BinaryMarshaler) error {
	if err := WriteType(w, typ); err != nil {
		return err
	}
	if err := EncodeLV(w, v); err != nil {
		return err
	}
	return nil
}

// EncodeLV encodes v to a binary format and writes the length-value record to w.
func EncodeLV(w io.Writer, v encoding.BinaryMarshaler) error {
	buf, err := v.MarshalBinary()
	if err != nil {
		return err
	}

	if err := WriteLV(w, buf); err != nil {
		return err
	}
	return nil
}

// DecodeTLV reads the type-length-value record from r and unmarshals it into v.
func DecodeTLV(r io.Reader, v encoding.BinaryUnmarshaler) (typ byte, err error) {
	typ, err = ReadType(r)
	if err != nil {
		return 0, err
	}
	if err := DecodeLV(r, v); err != nil {
		return 0, err
	}
	return typ, nil
}

// DecodeLV reads the length-value record from r and unmarshals it into v.
func DecodeLV(r io.Reader, v encoding.BinaryUnmarshaler) error {
	buf, err := ReadLV(r)
	if err != nil {
		return err
	}

	if err := v.UnmarshalBinary(buf); err != nil {
		return err
	}
	return nil
}
