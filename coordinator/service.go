package coordinator

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/coordinator/rpc"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// MuxHeader is the header byte used in the TCP mux.
const MuxHeader = rpc.MuxHeader

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
	statJoinClusterReq      = "joinClusterReq"
	statLeaveClusterReq     = "leaveClusterReq"
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

	HTTPDService interface {
		Addr() net.Addr
	}

	MetaClient interface {
		NodeID() uint64
		MetaServers() []string
		SetMetaServers(a []string)
		DataNode(id uint64) (*meta.NodeInfo, error)
		CreateDataNode(httpAddr, tcpAddr string) (*meta.NodeInfo, error)
		DataNodeByTCPAddr(tcpAddr string) (*meta.NodeInfo, error)
		Status() (*meta.MetaNodeStatus, error)
		RemoteAddr(addr string) string
		Save() error
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

	if err := s.openHTTPListener(); err != nil {
		return err
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
	if s.config.ClusterTracing {
		s.Logger = log.With(zap.String("service", "coordinator"))
	}
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
	JoinClusterReq      int64
	LeaveClusterReq     int64
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
			statJoinClusterReq:      atomic.LoadInt64(&s.stats.JoinClusterReq),
			statLeaveClusterReq:     atomic.LoadInt64(&s.stats.LeaveClusterReq),
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
		typ, err := rpc.ReadType(conn)
		if err != nil {
			if strings.HasSuffix(err.Error(), "EOF") {
				return
			}
			s.Logger.Error("Unable to read type", zap.Error(err))
			return
		}

		// Delegate message processing by type.
		switch typ {
		case rpc.WriteShardRequestMessage:
			buf, err := rpc.ReadLV(conn)
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
		case rpc.ExecuteStatementRequestMessage:
			buf, err := rpc.ReadLV(conn)
			if err != nil {
				s.Logger.Error("Unable to read length-value", zap.Error(err))
				return
			}
			err = s.processExecuteStatementRequest(buf)
			if err != nil {
				s.Logger.Error("Process execute statement error", zap.Error(err))
			}
			s.executeStatementResponse(conn, err)
		case rpc.CreateIteratorRequestMessage:
			atomic.AddInt64(&s.stats.CreateIteratorReq, 1)
			s.processCreateIteratorRequest(conn)
			return
		case rpc.IteratorCostRequestMessage:
			atomic.AddInt64(&s.stats.IteratorCostReq, 1)
			s.processIteratorCostRequest(conn)
			return
		case rpc.FieldDimensionsRequestMessage:
			atomic.AddInt64(&s.stats.FieldDimensionsReq, 1)
			s.processFieldDimensionsRequest(conn)
			return
		case rpc.MapTypeRequestMessage:
			atomic.AddInt64(&s.stats.MapTypeReq, 1)
			s.processMapTypeRequest(conn)
			return
		case rpc.ExpandSourcesRequestMessage:
			atomic.AddInt64(&s.stats.ExpandSourcesReq, 1)
			s.processExpandSourcesRequest(conn)
			return
		case rpc.BackupShardRequestMessage:
			atomic.AddInt64(&s.stats.BackupShardReq, 1)
			s.processBackupShardRequest(conn)
			return
		case rpc.CopyShardRequestMessage:
			atomic.AddInt64(&s.stats.CopyShardReq, 1)
			s.processCopyShardRequest(conn)
			return
		case rpc.RemoveShardRequestMessage:
			atomic.AddInt64(&s.stats.RemoveShardReq, 1)
			s.processRemoveShardRequest(conn)
			return
		case rpc.JoinClusterRequestMessage:
			atomic.AddInt64(&s.stats.JoinClusterReq, 1)
			s.processJoinClusterRequest(conn)
			return
		case rpc.LeaveClusterRequestMessage:
			atomic.AddInt64(&s.stats.LeaveClusterReq, 1)
			s.processLeaveClusterRequest(conn)
			return
		default:
			s.Logger.Warn("Coordinator service message type not found", zap.Uint8("Type", typ))
		}
	}
}

func (s *Service) processExecuteStatementRequest(buf []byte) error {
	// Unmarshal the request.
	var req rpc.ExecuteStatementRequest
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
	var resp rpc.ExecuteStatementResponse
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
	if err := rpc.WriteTLV(w, rpc.ExecuteStatementResponseMessage, buf); err != nil {
		s.Logger.Error("Error writing ExecuteStatement response", zap.Error(err))
	}
}

func (s *Service) processWriteShardRequest(buf []byte) error {
	// Build request
	var req rpc.WriteShardRequest
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
	var resp rpc.WriteShardResponse
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
	if err := rpc.WriteTLV(w, rpc.WriteShardResponseMessage, buf); err != nil {
		s.Logger.Error("Error writing WriteShard response", zap.Error(err))
	}
}

func (s *Service) processCreateIteratorRequest(conn net.Conn) {
	var itr query.Iterator
	if err := func() error {
		// Parse request.
		var req rpc.CreateIteratorRequest
		if err := rpc.DecodeLV(conn, &req); err != nil {
			return err
		}

		// Collect a shard group with a list of shards for each shard.
		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if sg == nil {
			return nil
		}

		var i query.Iterator
		var err error
		ctx := context.Background()
		m := &req.Measurement

		// Generate a single iterator from all shards.
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
					inputs = append(inputs, input)
				}
				return nil
			}(); err != nil {
				query.Iterators(inputs).Close()
				return err
			}

			i, err = query.Iterators(inputs).Merge(req.Opt)
		} else {
			i, err = sg.CreateIterator(ctx, m, req.Opt)
		}

		if err != nil {
			return err
		}
		itr = i

		return nil
	}(); err != nil {
		if itr != nil {
			itr.Close()
		}
		s.Logger.Error("Error reading CreateIterator request", zap.Error(err))
		rpc.EncodeTLV(conn, rpc.CreateIteratorResponseMessage, &rpc.CreateIteratorResponse{Err: err})
		return
	}

	resp := rpc.CreateIteratorResponse{}
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
	if err := rpc.EncodeTLV(conn, rpc.CreateIteratorResponseMessage, &resp); err != nil {
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
	var cost query.IteratorCost
	if err := func() error {
		// Parse request.
		var req rpc.IteratorCostRequest
		if err := rpc.DecodeLV(conn, &req); err != nil {
			return err
		}

		// Collect a shard group with a list of shards for each shard.
		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if sg == nil {
			return nil
		}

		// Calculate iterator cost from all shards.
		if req.Measurement.Regex != nil {
			var costs query.IteratorCost
			measurements := sg.MeasurementsByRegex(req.Measurement.Regex.Val)
			for _, measurement := range measurements {
				ic, err := sg.IteratorCost(measurement, req.Opt)
				if err != nil {
					return err
				}
				costs = costs.Combine(ic)
			}
			cost = costs
			return nil
		}

		ic, err := sg.IteratorCost(req.Measurement.Name, req.Opt)
		if err != nil {
			return err
		}
		cost = ic

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading IteratorCost request", zap.Error(err))
		rpc.EncodeTLV(conn, rpc.IteratorCostResponseMessage, &rpc.IteratorCostResponse{Err: err})
		return
	}

	// Encode success response.
	if err := rpc.EncodeTLV(conn, rpc.IteratorCostResponseMessage, &rpc.IteratorCostResponse{
		Cost: cost,
	}); err != nil {
		s.Logger.Error("Error writing IteratorCost response", zap.Error(err))
		return
	}
}

func (s *Service) processFieldDimensionsRequest(conn net.Conn) {
	var fields map[string]influxql.DataType
	var dimensions map[string]struct{}
	if err := func() error {
		// Parse request.
		var req rpc.FieldDimensionsRequest
		if err := rpc.DecodeLV(conn, &req); err != nil {
			return err
		}

		// Collect a shard group with a list of shards for each shard.
		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if sg == nil {
			return nil
		}

		// Calculate fields, dimensions from all shards.
		var measurements []string
		if req.Measurement.Regex != nil {
			measurements = sg.MeasurementsByRegex(req.Measurement.Regex.Val)
		} else {
			measurements = []string{req.Measurement.Name}
		}

		f, d, err := sg.FieldDimensions(measurements)
		if err != nil {
			return err
		}
		fields, dimensions = f, d

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading FieldDimensions request", zap.Error(err))
		rpc.EncodeTLV(conn, rpc.FieldDimensionsResponseMessage, &rpc.FieldDimensionsResponse{Err: err})
		return
	}

	// Encode success response.
	if err := rpc.EncodeTLV(conn, rpc.FieldDimensionsResponseMessage, &rpc.FieldDimensionsResponse{
		Fields:     fields,
		Dimensions: dimensions,
	}); err != nil {
		s.Logger.Error("Error writing FieldDimensions response", zap.Error(err))
		return
	}
}

func (s *Service) processMapTypeRequest(conn net.Conn) {
	var typ influxql.DataType
	if err := func() error {
		// Parse request.
		var req rpc.MapTypeRequest
		if err := rpc.DecodeLV(conn, &req); err != nil {
			return err
		}

		// Collect a shard group with a list of shards for each shard.
		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if sg == nil {
			typ = influxql.Unknown
			return nil
		}

		// Calculate data type from all shards.
		var names []string
		if req.Measurement.Regex != nil {
			names = sg.MeasurementsByRegex(req.Measurement.Regex.Val)
		} else {
			names = []string{req.Measurement.Name}
		}

		for _, name := range names {
			if req.Measurement.SystemIterator != "" {
				name = req.Measurement.SystemIterator
			}
			t := sg.MapType(name, req.Field)
			if typ.LessThan(t) {
				typ = t
			}
		}

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading MapType request", zap.Error(err))
		rpc.EncodeTLV(conn, rpc.MapTypeResponseMessage, &rpc.MapTypeResponse{Err: err})
		return
	}

	// Encode success response.
	if err := rpc.EncodeTLV(conn, rpc.MapTypeResponseMessage, &rpc.MapTypeResponse{
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
		var req rpc.ExpandSourcesRequest
		if err := rpc.DecodeLV(conn, &req); err != nil {
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
		rpc.EncodeTLV(conn, rpc.ExpandSourcesResponseMessage, &rpc.ExpandSourcesResponse{Err: err})
		return
	}

	// Encode success response.
	if err := rpc.EncodeTLV(conn, rpc.ExpandSourcesResponseMessage, &rpc.ExpandSourcesResponse{
		Sources: sources,
	}); err != nil {
		s.Logger.Error("Error writing ExpandSources response", zap.Error(err))
		return
	}
}

func (s *Service) processBackupShardRequest(conn net.Conn) {
	if err := func() error {
		// Parse request.
		var req rpc.BackupShardRequest
		if err := rpc.DecodeLV(conn, &req); err != nil {
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
		var req rpc.CopyShardRequest
		if err := rpc.DecodeLV(conn, &req); err != nil {
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
		rpc.EncodeTLV(conn, rpc.CopyShardResponseMessage, &rpc.CopyShardResponse{Err: err})
		return
	}

	// Encode success response.
	if err := rpc.EncodeTLV(conn, rpc.CopyShardResponseMessage, &rpc.CopyShardResponse{}); err != nil {
		s.Logger.Error("Error writing CopyShard response", zap.Error(err))
		return
	}
}

// backupRemoteShard connects to a coordinator service on a remote host and streams a shard.
func (s *Service) backupRemoteShard(host string, shardID uint64, since time.Time) (io.ReadCloser, error) {
	conn, err := net.Dial("tcp", host)
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
		if err := rpc.EncodeTLV(conn, rpc.BackupShardRequestMessage, &rpc.BackupShardRequest{
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
		var req rpc.RemoveShardRequest
		if err := rpc.DecodeLV(conn, &req); err != nil {
			return err
		}

		// Remove local shard.
		if err := s.TSDBStore.DeleteShard(req.ShardID); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading RemoveShard request", zap.Error(err))
		rpc.EncodeTLV(conn, rpc.RemoveShardResponseMessage, &rpc.RemoveShardResponse{Err: err})
		return
	}

	// Encode success response.
	if err := rpc.EncodeTLV(conn, rpc.RemoveShardResponseMessage, &rpc.RemoveShardResponse{}); err != nil {
		s.Logger.Error("Error writing RemoveShard response", zap.Error(err))
		return
	}
}

func (s *Service) processJoinClusterRequest(conn net.Conn) {
	if err := func() error {
		// Parse request.
		var req rpc.JoinClusterRequest
		if err := rpc.DecodeLV(conn, &req); err != nil {
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
			return errors.New("no leader")
		}

		if req.Update {
			// Only check whether the current tcp addr already exists
			tries, maxTries := 0, 100
			_, err = s.MetaClient.DataNodeByTCPAddr(s.TCPAddr())
			for err != nil && tries < maxTries {
				tries += 1
				time.Sleep(100 * time.Millisecond)
				_, err = s.MetaClient.DataNodeByTCPAddr(s.TCPAddr())
			}
			if err != nil {
				return fmt.Errorf("unable to update data node after %d retries: %s", maxTries, err)
			}
		} else {
			// If we've already created a data node for our id, we're done
			if _, err = s.MetaClient.DataNode(s.MetaClient.NodeID()); err == nil {
				return nil
			}

			tries, maxTries := 0, 10
			_, err = s.MetaClient.CreateDataNode(s.HTTPAddr(), s.TCPAddr())
			for err != nil && tries < maxTries {
				tries += 1
				time.Sleep(time.Second)
				_, err = s.MetaClient.CreateDataNode(s.HTTPAddr(), s.TCPAddr())
			}
			if err != nil {
				return fmt.Errorf("unable to create data node after %d retries: %s", maxTries, err)
			}
		}

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading JoinCluster request", zap.Error(err))
		rpc.EncodeTLV(conn, rpc.JoinClusterResponseMessage, &rpc.JoinClusterResponse{Err: err})
		return
	}

	// Encode success response.
	if err := rpc.EncodeTLV(conn, rpc.JoinClusterResponseMessage, &rpc.JoinClusterResponse{}); err != nil {
		s.Logger.Error("Error writing JoinCluster response", zap.Error(err))
		return
	}
}

func (s *Service) processLeaveClusterRequest(conn net.Conn) {
	if err := func() error {
		tries, maxTries := 0, 100
		_, err := s.MetaClient.DataNodeByTCPAddr(s.TCPAddr())
		for err == nil && tries < maxTries {
			tries += 1
			time.Sleep(100 * time.Millisecond)
			_, err = s.MetaClient.DataNodeByTCPAddr(s.TCPAddr())
		}
		if err == nil {
			s.Logger.Warn(fmt.Sprintf("Data node %s still exists, already tried to check leaved %d times", s.TCPAddr(), maxTries))
		}
		s.MetaClient.SetMetaServers(nil)
		if err := s.MetaClient.Save(); err != nil {
			s.Logger.Error("Error saving meta servers", zap.Error(err))
		}

		return nil
	}(); err != nil {
		s.Logger.Error("Error reading LeaveCluster request", zap.Error(err))
		rpc.EncodeTLV(conn, rpc.LeaveClusterResponseMessage, &rpc.LeaveClusterResponse{Err: err})
		return
	}

	// Encode success response.
	if err := rpc.EncodeTLV(conn, rpc.LeaveClusterResponseMessage, &rpc.LeaveClusterResponse{}); err != nil {
		s.Logger.Error("Error writing LeaveCluster response", zap.Error(err))
		return
	}
}

// openHTTPListener opens http listener.
func (s *Service) openHTTPListener() error {
	tries, maxTries := 0, 100
	addr := s.DefaultListener.Addr()
	for addr == nil && tries < maxTries {
		tries += 1
		time.Sleep(100 * time.Millisecond)
		addr = s.DefaultListener.Addr()
	}
	if addr == nil {
		return errors.New("default listen addr is nil")
	}
	s.httpListener = newChanListener(addr)
	return nil
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

// HTTPAddr returns the HTTP address.
func (s *Service) HTTPAddr() string {
	addr := s.config.RemoteHTTPBindAddress
	tries, maxTries := 0, 10
	for (s.HTTPDService == nil || s.HTTPDService.Addr() == nil) && tries < maxTries {
		tries += 1
		time.Sleep(200 * time.Millisecond)
	}
	if tries < maxTries {
		addr = s.HTTPDService.Addr().String()
	}
	return s.MetaClient.RemoteAddr(addr)
}

// TCPAddr returns the TCP address.
func (s *Service) TCPAddr() string {
	addr := s.config.RemoteBindAddress
	tries, maxTries := 0, 10
	for (s.Listener == nil || s.Listener.Addr() == nil) && tries < maxTries {
		tries += 1
		time.Sleep(200 * time.Millisecond)
	}
	if tries < maxTries {
		addr = s.Listener.Addr().String()
	}
	return s.MetaClient.RemoteAddr(addr)
}
