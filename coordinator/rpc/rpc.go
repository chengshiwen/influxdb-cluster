package rpc

import (
	"encoding"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/coordinator/internal"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

// MaxMessageSize defines how large a message can be before we reject it
const MaxMessageSize = 1024 * 1024 * 1024 // 1GB

// MuxHeader is the header byte used for the TCP muxer.
const MuxHeader = 2

const (
	WriteShardRequestMessage byte = iota + 1
	WriteShardResponseMessage

	ExecuteStatementRequestMessage
	ExecuteStatementResponseMessage

	CreateIteratorRequestMessage
	CreateIteratorResponseMessage

	IteratorCostRequestMessage
	IteratorCostResponseMessage

	FieldDimensionsRequestMessage
	FieldDimensionsResponseMessage

	MapTypeRequestMessage
	MapTypeResponseMessage

	ExpandSourcesRequestMessage
	ExpandSourcesResponseMessage

	BackupShardRequestMessage
	BackupShardResponseMessage

	CopyShardRequestMessage
	CopyShardResponseMessage

	RemoveShardRequestMessage
	RemoveShardResponseMessage

	JoinClusterRequestMessage
	JoinClusterResponseMessage

	LeaveClusterRequestMessage
	LeaveClusterResponseMessage
)

// WriteShardRequest represents the a request to write a slice of points to a shard
type WriteShardRequest struct {
	pb internal.WriteShardRequest
}

// WriteShardResponse represents the response returned from a remote WriteShardRequest call
type WriteShardResponse struct {
	pb internal.WriteShardResponse
}

// SetShardID sets the ShardID
func (w *WriteShardRequest) SetShardID(id uint64) { w.pb.ShardID = &id }

// ShardID gets the ShardID
func (w *WriteShardRequest) ShardID() uint64 { return w.pb.GetShardID() }

func (w *WriteShardRequest) SetDatabase(db string) { w.pb.Database = &db }

func (w *WriteShardRequest) SetRetentionPolicy(rp string) { w.pb.RetentionPolicy = &rp }

func (w *WriteShardRequest) Database() string { return w.pb.GetDatabase() }

func (w *WriteShardRequest) RetentionPolicy() string { return w.pb.GetRetentionPolicy() }

// Points returns the time series Points
func (w *WriteShardRequest) Points() []models.Point { return w.unmarshalPoints() }

// AddPoint adds a new time series point
func (w *WriteShardRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags map[string]string) {
	pt, err := models.NewPoint(
		name, models.NewTags(tags), map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.AddPoints([]models.Point{pt})
}

// AddPoints adds a new time series point
func (w *WriteShardRequest) AddPoints(points []models.Point) {
	for _, p := range points {
		b, err := p.MarshalBinary()
		if err != nil {
			// A error here means that we create a point higher in the stack that we could
			// not marshal to a byte slice.  If that happens, the endpoint that created that
			// point needs to be fixed.
			panic(fmt.Sprintf("failed to marshal point: `%v`: %v", p, err))
		}
		w.pb.Points = append(w.pb.Points, b)
	}
}

// MarshalBinary encodes the object to a binary format.
func (w *WriteShardRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates WritePointRequest from a binary format.
func (w *WriteShardRequest) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

func (w *WriteShardRequest) unmarshalPoints() []models.Point {
	points := make([]models.Point, len(w.pb.GetPoints()))
	for i, p := range w.pb.GetPoints() {
		pt, err := models.NewPointFromBytes(p)
		if err != nil {
			// A error here means that one node created a valid point and sent us an
			// unparseable version.  We could log and drop the point and allow
			// anti-entropy to resolve the discrepancy, but this shouldn't ever happen.
			panic(fmt.Sprintf("failed to parse point: `%v`: %v", string(p), err))
		}

		points[i] = pt
	}
	return points
}

// SetCode sets the Code
func (w *WriteShardResponse) SetCode(code int) { w.pb.Code = proto.Int32(int32(code)) }

// SetMessage sets the Message
func (w *WriteShardResponse) SetMessage(message string) { w.pb.Message = &message }

// Code returns the Code
func (w *WriteShardResponse) Code() int { return int(w.pb.GetCode()) }

// Message returns the Message
func (w *WriteShardResponse) Message() string { return w.pb.GetMessage() }

// MarshalBinary encodes the object to a binary format.
func (w *WriteShardResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates WritePointRequest from a binary format.
func (w *WriteShardResponse) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

// ExecuteStatementRequest represents the a request to execute a statement on a node.
type ExecuteStatementRequest struct {
	pb internal.ExecuteStatementRequest
}

// Statement returns the InfluxQL statement.
func (r *ExecuteStatementRequest) Statement() string { return r.pb.GetStatement() }

// SetStatement sets the InfluxQL statement.
func (r *ExecuteStatementRequest) SetStatement(statement string) {
	r.pb.Statement = proto.String(statement)
}

// Database returns the database name.
func (r *ExecuteStatementRequest) Database() string { return r.pb.GetDatabase() }

// SetDatabase sets the database name.
func (r *ExecuteStatementRequest) SetDatabase(database string) {
	r.pb.Database = proto.String(database)
}

// MarshalBinary encodes the object to a binary format.
func (r *ExecuteStatementRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.pb)
}

// UnmarshalBinary populates ExecuteStatementRequest from a binary format.
func (r *ExecuteStatementRequest) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &r.pb); err != nil {
		return err
	}
	return nil
}

// ExecuteStatementResponse represents the response returned from a remote ExecuteStatementRequest call.
type ExecuteStatementResponse struct {
	pb internal.ExecuteStatementResponse
}

// Code returns the response code.
func (w *ExecuteStatementResponse) Code() int { return int(w.pb.GetCode()) }

// SetCode sets the Code
func (w *ExecuteStatementResponse) SetCode(code int) { w.pb.Code = proto.Int32(int32(code)) }

// Message returns the repsonse message.
func (w *ExecuteStatementResponse) Message() string { return w.pb.GetMessage() }

// SetMessage sets the Message
func (w *ExecuteStatementResponse) SetMessage(message string) { w.pb.Message = &message }

// MarshalBinary encodes the object to a binary format.
func (w *ExecuteStatementResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates ExecuteStatementResponse from a binary format.
func (w *ExecuteStatementResponse) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

// CreateIteratorRequest represents a request to create a remote iterator.
type CreateIteratorRequest struct {
	ShardIDs    []uint64
	Measurement influxql.Measurement
	Opt         query.IteratorOptions
}

// MarshalBinary encodes r to a binary format.
func (r *CreateIteratorRequest) MarshalBinary() ([]byte, error) {
	mBuf, err := r.Measurement.MarshalBinary()
	if err != nil {
		return nil, err
	}
	oBuf, err := r.Opt.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.CreateIteratorRequest{
		ShardIDs:    r.ShardIDs,
		Measurement: mBuf,
		Opt:         oBuf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *CreateIteratorRequest) UnmarshalBinary(data []byte) error {
	var pb internal.CreateIteratorRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Measurement.UnmarshalBinary(pb.GetMeasurement()); err != nil {
		return err
	}
	if err := r.Opt.UnmarshalBinary(pb.GetOpt()); err != nil {
		return err
	}
	return nil
}

// CreateIteratorResponse represents a response from remote iterator creation.
type CreateIteratorResponse struct {
	Err   error
	Type  influxql.DataType
	Stats query.IteratorStats
}

// MarshalBinary encodes r to a binary format.
func (r *CreateIteratorResponse) MarshalBinary() ([]byte, error) {
	var pb internal.CreateIteratorResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	pb.Type = proto.Int32(int32(r.Type))
	pb.Stats = &internal.IteratorStats{
		SeriesN: proto.Int64(int64(r.Stats.SeriesN)),
		PointN:  proto.Int64(int64(r.Stats.PointN)),
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *CreateIteratorResponse) UnmarshalBinary(data []byte) error {
	var pb internal.CreateIteratorResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	r.Type = influxql.DataType(pb.GetType())
	if stats := pb.GetStats(); stats != nil {
		r.Stats.SeriesN = int(stats.GetSeriesN())
		r.Stats.PointN = int(stats.GetPointN())
	}
	return nil
}

// IteratorCostRequest represents a request to create a remote iterator cost.
type IteratorCostRequest struct {
	ShardIDs    []uint64
	Measurement influxql.Measurement
	Opt         query.IteratorOptions
}

// MarshalBinary encodes r to a binary format.
func (r *IteratorCostRequest) MarshalBinary() ([]byte, error) {
	mBuf, err := r.Measurement.MarshalBinary()
	if err != nil {
		return nil, err
	}
	oBuf, err := r.Opt.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.IteratorCostRequest{
		ShardIDs:    r.ShardIDs,
		Measurement: mBuf,
		Opt:         oBuf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *IteratorCostRequest) UnmarshalBinary(data []byte) error {
	var pb internal.IteratorCostRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Measurement.UnmarshalBinary(pb.GetMeasurement()); err != nil {
		return err
	}
	if err := r.Opt.UnmarshalBinary(pb.GetOpt()); err != nil {
		return err
	}
	return nil
}

// IteratorCostResponse represents a response from remote iterator cost.
type IteratorCostResponse struct {
	Err  error
	Cost query.IteratorCost
}

// MarshalBinary encodes r to a binary format.
func (r *IteratorCostResponse) MarshalBinary() ([]byte, error) {
	var pb internal.IteratorCostResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	pb.Cost = &internal.IteratorCost{
		NumShards:    proto.Int64(r.Cost.NumShards),
		NumSeries:    proto.Int64(r.Cost.NumSeries),
		CachedValues: proto.Int64(r.Cost.CachedValues),
		NumFiles:     proto.Int64(r.Cost.NumFiles),
		BlocksRead:   proto.Int64(r.Cost.BlocksRead),
		BlockSize:    proto.Int64(r.Cost.BlockSize),
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *IteratorCostResponse) UnmarshalBinary(data []byte) error {
	var pb internal.IteratorCostResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	if cost := pb.GetCost(); cost != nil {
		r.Cost.NumShards = cost.GetNumShards()
		r.Cost.NumSeries = cost.GetNumSeries()
		r.Cost.CachedValues = cost.GetCachedValues()
		r.Cost.NumFiles = cost.GetNumFiles()
		r.Cost.BlocksRead = cost.GetBlocksRead()
		r.Cost.BlockSize = cost.GetBlockSize()
	}
	return nil
}

// FieldDimensionsRequest represents a request to retrieve unique fields & dimensions.
type FieldDimensionsRequest struct {
	ShardIDs    []uint64
	Measurement influxql.Measurement
}

// MarshalBinary encodes r to a binary format.
func (r *FieldDimensionsRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Measurement.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.FieldDimensionsRequest{
		ShardIDs:    r.ShardIDs,
		Measurement: buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *FieldDimensionsRequest) UnmarshalBinary(data []byte) error {
	var pb internal.FieldDimensionsRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Measurement.UnmarshalBinary(pb.GetMeasurement()); err != nil {
		return err
	}

	return nil
}

// FieldDimensionsResponse represents a response from remote iterator creation.
type FieldDimensionsResponse struct {
	Fields     map[string]influxql.DataType
	Dimensions map[string]struct{}
	Err        error
}

// MarshalBinary encodes r to a binary format.
func (r *FieldDimensionsResponse) MarshalBinary() ([]byte, error) {
	var pb internal.FieldDimensionsResponse

	buf, err := json.Marshal(r.Fields)
	if err != nil {
		return nil, err
	}
	pb.Fields = buf[:]

	pb.Dimensions = make([]string, 0, len(r.Dimensions))
	for k := range r.Dimensions {
		pb.Dimensions = append(pb.Dimensions, k)
	}

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *FieldDimensionsResponse) UnmarshalBinary(data []byte) error {
	var pb internal.FieldDimensionsResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	err := json.Unmarshal(pb.GetFields(), &r.Fields)
	if err != nil {
		return err
	}

	r.Dimensions = make(map[string]struct{}, len(pb.GetDimensions()))
	for _, s := range pb.GetDimensions() {
		r.Dimensions[s] = struct{}{}
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// MapTypeRequest represents a request to retrieve map type.
type MapTypeRequest struct {
	ShardIDs    []uint64
	Measurement influxql.Measurement
	Field       string
}

// MarshalBinary encodes r to a binary format.
func (r *MapTypeRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Measurement.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.MapTypeRequest{
		ShardIDs:    r.ShardIDs,
		Measurement: buf,
		Field:       proto.String(r.Field),
	})
}

// UnmarshalBinary decodes data into r.
func (r *MapTypeRequest) UnmarshalBinary(data []byte) error {
	var pb internal.MapTypeRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Measurement.UnmarshalBinary(pb.GetMeasurement()); err != nil {
		return err
	}
	r.Field = pb.GetField()

	return nil
}

// MapTypeResponse represents a response from remote map type.
type MapTypeResponse struct {
	Type influxql.DataType
	Err  error
}

// MarshalBinary encodes r to a binary format.
func (r *MapTypeResponse) MarshalBinary() ([]byte, error) {
	var pb internal.MapTypeResponse
	pb.Type = proto.Int32(int32(r.Type))
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *MapTypeResponse) UnmarshalBinary(data []byte) error {
	var pb internal.MapTypeResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	r.Type = influxql.DataType(pb.GetType())
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// ExpandSourcesRequest represents a request to expand regex sources.
type ExpandSourcesRequest struct {
	ShardIDs []uint64
	Sources  influxql.Sources
}

// MarshalBinary encodes r to a binary format.
func (r *ExpandSourcesRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Sources.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.ExpandSourcesRequest{
		ShardIDs: r.ShardIDs,
		Sources:  buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *ExpandSourcesRequest) UnmarshalBinary(data []byte) error {
	var pb internal.ExpandSourcesRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Sources.UnmarshalBinary(pb.GetSources()); err != nil {
		return err
	}
	return nil
}

// ExpandSourcesResponse represents a response from source expansion.
type ExpandSourcesResponse struct {
	Sources influxql.Sources
	Err     error
}

// MarshalBinary encodes r to a binary format.
func (r *ExpandSourcesResponse) MarshalBinary() ([]byte, error) {
	var pb internal.ExpandSourcesResponse
	buf, err := r.Sources.MarshalBinary()
	if err != nil {
		return nil, err
	}
	pb.Sources = buf

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *ExpandSourcesResponse) UnmarshalBinary(data []byte) error {
	var pb internal.ExpandSourcesResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if err := r.Sources.UnmarshalBinary(pb.GetSources()); err != nil {
		return err
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// BackupShardRequest represents a request to stream a backup of a single shard.
type BackupShardRequest struct {
	ShardID uint64
	Since   time.Time
}

// MarshalBinary encodes r to a binary format.
func (r *BackupShardRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&internal.BackupShardRequest{
		ShardID: proto.Uint64(r.ShardID),
		Since:   proto.Int64(r.Since.UnixNano()),
	})
}

// UnmarshalBinary decodes data into r.
func (r *BackupShardRequest) UnmarshalBinary(data []byte) error {
	var pb internal.BackupShardRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardID = pb.GetShardID()
	r.Since = time.Unix(0, pb.GetSince())
	return nil
}

// CopyShardRequest represents a request to copy a shard from another host.
type CopyShardRequest struct {
	Host     string
	Database string
	Policy   string
	ShardID  uint64
	Since    time.Time
}

// MarshalBinary encodes r to a binary format.
func (r *CopyShardRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&internal.CopyShardRequest{
		Host:     proto.String(r.Host),
		Database: proto.String(r.Database),
		Policy:   proto.String(r.Policy),
		ShardID:  proto.Uint64(r.ShardID),
		Since:    proto.Int64(r.Since.UnixNano()),
	})
}

// UnmarshalBinary decodes data into r.
func (r *CopyShardRequest) UnmarshalBinary(data []byte) error {
	var pb internal.CopyShardRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.Host = pb.GetHost()
	r.Database = pb.GetDatabase()
	r.Policy = pb.GetPolicy()
	r.ShardID = pb.GetShardID()
	r.Since = time.Unix(0, pb.GetSince())
	return nil
}

// CopyShardResponse represents a response from a shard copy.
type CopyShardResponse struct {
	Err error
}

func (r *CopyShardResponse) MarshalBinary() ([]byte, error) {
	var pb internal.CopyShardResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

func (r *CopyShardResponse) UnmarshalBinary(data []byte) error {
	var pb internal.CopyShardResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// RemoveShardRequest represents a request to remove a shard.
type RemoveShardRequest struct {
	ShardID uint64
}

// MarshalBinary encodes r to a binary format.
func (r *RemoveShardRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&internal.RemoveShardRequest{
		ShardID: proto.Uint64(r.ShardID),
	})
}

// UnmarshalBinary decodes data into r.
func (r *RemoveShardRequest) UnmarshalBinary(data []byte) error {
	var pb internal.RemoveShardRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardID = pb.GetShardID()
	return nil
}

// RemoveShardResponse represents a response from a shard remove.
type RemoveShardResponse struct {
	Err error
}

func (r *RemoveShardResponse) MarshalBinary() ([]byte, error) {
	var pb internal.RemoveShardResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

func (r *RemoveShardResponse) UnmarshalBinary(data []byte) error {
	var pb internal.RemoveShardResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// JoinClusterRequest represents a request to join cluster.
type JoinClusterRequest struct {
	MetaServers []string
	Update      bool
}

// MarshalBinary encodes r to a binary format.
func (r *JoinClusterRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&internal.JoinClusterRequest{
		MetaServers: r.MetaServers,
		Update:      proto.Bool(r.Update),
	})
}

// UnmarshalBinary decodes data into r.
func (r *JoinClusterRequest) UnmarshalBinary(data []byte) error {
	var pb internal.JoinClusterRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.MetaServers = pb.GetMetaServers()
	r.Update = pb.GetUpdate()
	return nil
}

// JoinClusterResponse represents a response to join cluster.
type JoinClusterResponse struct {
	Err error
}

func (r *JoinClusterResponse) MarshalBinary() ([]byte, error) {
	var pb internal.JoinClusterResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

func (r *JoinClusterResponse) UnmarshalBinary(data []byte) error {
	var pb internal.JoinClusterResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// LeaveClusterResponse represents a response to leave cluster.
type LeaveClusterResponse struct {
	Err error
}

func (r *LeaveClusterResponse) MarshalBinary() ([]byte, error) {
	var pb internal.LeaveClusterResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

func (r *LeaveClusterResponse) UnmarshalBinary(data []byte) error {
	var pb internal.LeaveClusterResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
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
