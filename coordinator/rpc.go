package coordinator

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"log"
	"net"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/coordinator/internal"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

//go:generate protoc --gogo_out=. internal/data.proto

// WriteShardRequest represents a request to write a slice of points to a shard
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

// SetBinaryPoints sets the time series binary points
func (w *WriteShardRequest) SetBinaryPoints(points [][]byte) { w.pb.Points = points }

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
			log.Printf("failed to marshal point: `%v`: %v", p, err)
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
			log.Printf("failed to parse point: `%v`: %v", string(p), err)
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

// ExecuteStatementRequest represents a request to execute a statement on a node.
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

// TaskManagerStatementRequest represents a request to execute a task manager statement on a node.
type TaskManagerStatementRequest struct {
	Statement string
}

// MarshalBinary encodes r to a binary format.
func (r *TaskManagerStatementRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&internal.TaskManagerStatementRequest{
		Statement: proto.String(r.Statement),
	})
}

// UnmarshalBinary decodes data into r.
func (r *TaskManagerStatementRequest) UnmarshalBinary(data []byte) error {
	var pb internal.TaskManagerStatementRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	r.Statement = pb.GetStatement()
	return nil
}

// TaskManagerStatementResponse represents a response to task manager statement request.
type TaskManagerStatementResponse struct {
	Result query.Result
	Err    error
}

func (r *TaskManagerStatementResponse) MarshalBinary() ([]byte, error) {
	var pb internal.TaskManagerStatementResponse
	buf, err := r.Result.MarshalJSON()
	if err != nil {
		return nil, err
	}
	pb.Result = buf[:]
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

func (r *TaskManagerStatementResponse) UnmarshalBinary(data []byte) error {
	var pb internal.TaskManagerStatementResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	err := r.Result.UnmarshalJSON(pb.GetResult())
	if err != nil {
		return err
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// MeasurementNamesRequest represents a request to retrieve measurement names.
type MeasurementNamesRequest struct {
	Database        string
	RetentionPolicy string
	Condition       influxql.Expr
}

// MarshalBinary encodes r to a binary format.
func (r *MeasurementNamesRequest) MarshalBinary() ([]byte, error) {
	var condition string
	if r.Condition != nil {
		condition = r.Condition.String()
	}
	return proto.Marshal(&internal.MeasurementNamesRequest{
		Database:        proto.String(r.Database),
		RetentionPolicy: proto.String(r.RetentionPolicy),
		Condition:       proto.String(condition),
	})
}

// UnmarshalBinary decodes data into r.
func (r *MeasurementNamesRequest) UnmarshalBinary(data []byte) error {
	var pb internal.MeasurementNamesRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.Database = pb.GetDatabase()
	r.RetentionPolicy = pb.GetRetentionPolicy()
	if pb.GetCondition() != "" {
		if condition, err := influxql.ParseExpr(pb.GetCondition()); err != nil {
			return err
		} else {
			r.Condition = condition
		}
	}
	return nil
}

// MeasurementNamesResponse represents a response from measurement names.
type MeasurementNamesResponse struct {
	Names [][]byte
	Err   error
}

// MarshalBinary encodes r to a binary format.
func (r *MeasurementNamesResponse) MarshalBinary() ([]byte, error) {
	var pb internal.MeasurementNamesResponse
	pb.Names = r.Names
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *MeasurementNamesResponse) UnmarshalBinary(data []byte) error {
	var pb internal.MeasurementNamesResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	r.Names = pb.GetNames()
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// TagKeysRequest represents a request to retrieve tag keys.
type TagKeysRequest struct {
	ShardIDs  []uint64
	Condition influxql.Expr
}

// MarshalBinary encodes r to a binary format.
func (r *TagKeysRequest) MarshalBinary() ([]byte, error) {
	var condition string
	if r.Condition != nil {
		condition = r.Condition.String()
	}
	return proto.Marshal(&internal.TagKeysRequest{
		ShardIDs:  r.ShardIDs,
		Condition: proto.String(condition),
	})
}

// UnmarshalBinary decodes data into r.
func (r *TagKeysRequest) UnmarshalBinary(data []byte) error {
	var pb internal.TagKeysRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if pb.GetCondition() != "" {
		if condition, err := influxql.ParseExpr(pb.GetCondition()); err != nil {
			return err
		} else {
			r.Condition = condition
		}
	}
	return nil
}

// TagKeysResponse represents a response from tag keys.
type TagKeysResponse struct {
	TagKeys []tsdb.TagKeys
	Err     error
}

// MarshalBinary encodes r to a binary format.
func (r *TagKeysResponse) MarshalBinary() ([]byte, error) {
	var pb internal.TagKeysResponse
	buf, err := json.Marshal(r.TagKeys)
	if err != nil {
		return nil, err
	}
	pb.TagKeys = buf

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *TagKeysResponse) UnmarshalBinary(data []byte) error {
	var pb internal.TagKeysResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	err := json.Unmarshal(pb.GetTagKeys(), &r.TagKeys)
	if err != nil {
		return err
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// TagValuesRequest represents a request to retrieve tag values.
type TagValuesRequest struct {
	ShardIDs  []uint64
	Condition influxql.Expr
}

// MarshalBinary encodes r to a binary format.
func (r *TagValuesRequest) MarshalBinary() ([]byte, error) {
	var condition string
	if r.Condition != nil {
		condition = r.Condition.String()
	}
	return proto.Marshal(&internal.TagValuesRequest{
		ShardIDs:  r.ShardIDs,
		Condition: proto.String(condition),
	})
}

// UnmarshalBinary decodes data into r.
func (r *TagValuesRequest) UnmarshalBinary(data []byte) error {
	var pb internal.TagValuesRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if pb.GetCondition() != "" {
		if condition, err := influxql.ParseExpr(pb.GetCondition()); err != nil {
			return err
		} else {
			r.Condition = condition
		}
	}
	return nil
}

// TagValuesResponse represents a response from tag values.
type TagValuesResponse struct {
	TagValues []tsdb.TagValues
	Err       error
}

// MarshalBinary encodes r to a binary format.
func (r *TagValuesResponse) MarshalBinary() ([]byte, error) {
	var pb internal.TagValuesResponse
	buf, err := json.Marshal(r.TagValues)
	if err != nil {
		return nil, err
	}
	pb.TagValues = buf

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *TagValuesResponse) UnmarshalBinary(data []byte) error {
	var pb internal.TagValuesResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	err := json.Unmarshal(pb.GetTagValues(), &r.TagValues)
	if err != nil {
		return err
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// SeriesSketchesRequest represents a request to retrieve series sketches.
type SeriesSketchesRequest struct {
	Database string
}

// MarshalBinary encodes r to a binary format.
func (r *SeriesSketchesRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&internal.SeriesSketchesRequest{
		Database: proto.String(r.Database),
	})
}

// UnmarshalBinary decodes data into r.
func (r *SeriesSketchesRequest) UnmarshalBinary(data []byte) error {
	var pb internal.SeriesSketchesRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	r.Database = pb.GetDatabase()
	return nil
}

// SeriesSketchesResponse represents a response from series sketches.
type SeriesSketchesResponse struct {
	Sketch   estimator.Sketch
	TSSketch estimator.Sketch
	Err      error
}

func (r *SeriesSketchesResponse) MarshalBinary() ([]byte, error) {
	var pb internal.SeriesSketchesResponse
	if r.Sketch != nil {
		buf, err := r.Sketch.MarshalBinary()
		if err != nil {
			return nil, err
		}
		pb.Sketch = buf
	}

	if r.TSSketch != nil {
		tsBuf, err := r.TSSketch.MarshalBinary()
		if err != nil {
			return nil, err
		}
		pb.TSSketch = tsBuf
	}

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

func (r *SeriesSketchesResponse) UnmarshalBinary(data []byte) error {
	var pb internal.SeriesSketchesResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	if sketch := pb.GetSketch(); len(sketch) > 0 {
		r.Sketch = &hll.Plus{}
		if err := r.Sketch.UnmarshalBinary(sketch); err != nil {
			return err
		}
	}

	if tsSketch := pb.GetTSSketch(); len(tsSketch) > 0 {
		r.TSSketch = &hll.Plus{}
		if err := r.TSSketch.UnmarshalBinary(tsSketch); err != nil {
			return err
		}
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// MeasurementsSketchesRequest represents a request to retrieve measurements sketches.
type MeasurementsSketchesRequest struct {
	Database string
}

// MarshalBinary encodes r to a binary format.
func (r *MeasurementsSketchesRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&internal.MeasurementsSketchesRequest{
		Database: proto.String(r.Database),
	})
}

// UnmarshalBinary decodes data into r.
func (r *MeasurementsSketchesRequest) UnmarshalBinary(data []byte) error {
	var pb internal.MeasurementsSketchesRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	r.Database = pb.GetDatabase()
	return nil
}

// MeasurementsSketchesResponse represents a response from measurements sketches.
type MeasurementsSketchesResponse struct {
	Sketch   estimator.Sketch
	TSSketch estimator.Sketch
	Err      error
}

func (r *MeasurementsSketchesResponse) MarshalBinary() ([]byte, error) {
	var pb internal.MeasurementsSketchesResponse
	if r.Sketch != nil {
		buf, err := r.Sketch.MarshalBinary()
		if err != nil {
			return nil, err
		}
		pb.Sketch = buf
	}

	if r.TSSketch != nil {
		tsBuf, err := r.TSSketch.MarshalBinary()
		if err != nil {
			return nil, err
		}
		pb.TSSketch = tsBuf
	}

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

func (r *MeasurementsSketchesResponse) UnmarshalBinary(data []byte) error {
	var pb internal.MeasurementsSketchesResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	if sketch := pb.GetSketch(); len(sketch) > 0 {
		r.Sketch = &hll.Plus{}
		if err := r.Sketch.UnmarshalBinary(sketch); err != nil {
			return err
		}
	}

	if tsSketch := pb.GetTSSketch(); len(tsSketch) > 0 {
		r.TSSketch = &hll.Plus{}
		if err := r.TSSketch.UnmarshalBinary(tsSketch); err != nil {
			return err
		}
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// StoreReadFilterRequest represents a request to read filter.
type StoreReadFilterRequest struct {
	ShardIDs []uint64
	Request  datatypes.ReadFilterRequest
}

// MarshalBinary encodes r to a binary format.
func (r *StoreReadFilterRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Request.Marshal()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.StoreReadFilterRequest{
		ShardIDs: r.ShardIDs,
		Request:  buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *StoreReadFilterRequest) UnmarshalBinary(data []byte) error {
	var pb internal.StoreReadFilterRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	r.ShardIDs = pb.GetShardIDs()
	if err := r.Request.Unmarshal(pb.GetRequest()); err != nil {
		return err
	}
	return nil
}

// StoreReadFilterResponse represents a response from remote read filter.
type StoreReadFilterResponse struct {
	Err error
}

// MarshalBinary encodes r to a binary format.
func (r *StoreReadFilterResponse) MarshalBinary() ([]byte, error) {
	var pb internal.StoreReadFilterResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *StoreReadFilterResponse) UnmarshalBinary(data []byte) error {
	var pb internal.StoreReadFilterResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// StoreReadGroupRequest represents a request to read group.
type StoreReadGroupRequest struct {
	ShardIDs []uint64
	Request  datatypes.ReadGroupRequest
}

// MarshalBinary encodes r to a binary format.
func (r *StoreReadGroupRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Request.Marshal()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.StoreReadGroupRequest{
		ShardIDs: r.ShardIDs,
		Request:  buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *StoreReadGroupRequest) UnmarshalBinary(data []byte) error {
	var pb internal.StoreReadGroupRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	r.ShardIDs = pb.GetShardIDs()
	if err := r.Request.Unmarshal(pb.GetRequest()); err != nil {
		return err
	}
	return nil
}

// StoreReadGroupResponse represents a response from remote read group.
type StoreReadGroupResponse struct {
	Err error
}

// MarshalBinary encodes r to a binary format.
func (r *StoreReadGroupResponse) MarshalBinary() ([]byte, error) {
	var pb internal.StoreReadGroupResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *StoreReadGroupResponse) UnmarshalBinary(data []byte) error {
	var pb internal.StoreReadGroupResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// CreateIteratorRequest represents a request to create a remote iterator.
type CreateIteratorRequest struct {
	ShardIDs    []uint64
	Measurement influxql.Measurement
	Opt         query.IteratorOptions
	SpanContext tracing.SpanContext
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
	sBuf, err := r.SpanContext.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.CreateIteratorRequest{
		ShardIDs:    r.ShardIDs,
		Measurement: mBuf,
		Opt:         oBuf,
		SpanContext: sBuf,
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
	if err := r.SpanContext.UnmarshalBinary(pb.GetSpanContext()); err != nil {
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

// FieldDimensionsResponse represents a response from remote fields & dimensions.
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

// ListShardsResponse represents a response to list shards.
type ListShardsResponse struct {
	Shards map[uint64]*meta.ShardOwnerInfo
	Err    error
}

func (r *ListShardsResponse) MarshalBinary() ([]byte, error) {
	var pb internal.ListShardsResponse
	buf, err := json.Marshal(r.Shards)
	if err != nil {
		return nil, err
	}
	pb.Shards = buf[:]
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

func (r *ListShardsResponse) UnmarshalBinary(data []byte) error {
	var pb internal.ListShardsResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	err := json.Unmarshal(pb.GetShards(), &r.Shards)
	if err != nil {
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
	Node *meta.NodeInfo
	Err  error
}

func (r *JoinClusterResponse) MarshalBinary() ([]byte, error) {
	var pb internal.JoinClusterResponse
	if r.Node != nil {
		pb.Node = &internal.NodeInfo{
			ID:      proto.Uint64(r.Node.ID),
			Addr:    proto.String(r.Node.Addr),
			TCPAddr: proto.String(r.Node.TCPAddr),
		}
	}
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
	if node := pb.GetNode(); node != nil {
		r.Node = &meta.NodeInfo{
			ID:      node.GetID(),
			Addr:    node.GetAddr(),
			TCPAddr: node.GetTCPAddr(),
		}
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

// RemoveHintedHandoffRequest represents a request to remove a hinted handoff.
type RemoveHintedHandoffRequest struct {
	NodeID uint64
}

// MarshalBinary encodes r to a binary format.
func (r *RemoveHintedHandoffRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&internal.RemoveHintedHandoffRequest{
		NodeID: proto.Uint64(r.NodeID),
	})
}

// UnmarshalBinary decodes data into r.
func (r *RemoveHintedHandoffRequest) UnmarshalBinary(data []byte) error {
	var pb internal.RemoveHintedHandoffRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	r.NodeID = pb.GetNodeID()
	return nil
}

// RemoveHintedHandoffResponse represents a response from a hinted handoff remove.
type RemoveHintedHandoffResponse struct {
	Err error
}

func (r *RemoveHintedHandoffResponse) MarshalBinary() ([]byte, error) {
	var pb internal.RemoveHintedHandoffResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

func (r *RemoveHintedHandoffResponse) UnmarshalBinary(data []byte) error {
	var pb internal.RemoveHintedHandoffResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// Client provides an API for the rpc service.
type Client struct {
	tlsConfig *tls.Config
	timeout   time.Duration
}

// NewClient returns a new *Client.
func NewClient(tlsConfig *tls.Config, timeout time.Duration) *Client {
	return &Client{
		tlsConfig: tlsConfig,
		timeout:   timeout,
	}
}

func (c *Client) dial(address string) (net.Conn, error) {
	return tcp.DialTLSTimeoutHeader("tcp", address, c.tlsConfig, c.timeout, MuxHeader)
}

func (c *Client) CopyShard(address, host, database, policy string, shardID uint64, since time.Time) error {
	conn, err := c.dial(address)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send request.
	req := CopyShardRequest{
		Host:     host,
		Database: database,
		Policy:   policy,
		ShardID:  shardID,
		Since:    since,
	}
	err = EncodeTLV(conn, copyShardRequestMessage, &req)
	if err != nil {
		return err
	}

	// Read the response.
	_, buf, err := ReadTLV(conn)
	if err != nil {
		return err
	}

	// Unmarshal response.
	var resp CopyShardResponse
	if err = resp.UnmarshalBinary(buf); err != nil {
		return err
	}
	return resp.Err
}

func (c *Client) RemoveShard(address string, shardID uint64) error {
	conn, err := c.dial(address)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send request.
	req := RemoveShardRequest{
		ShardID: shardID,
	}
	err = EncodeTLV(conn, removeShardRequestMessage, &req)
	if err != nil {
		return err
	}

	// Read the response.
	_, buf, err := ReadTLV(conn)
	if err != nil {
		return err
	}

	// Unmarshal response.
	var resp RemoveShardResponse
	if err = resp.UnmarshalBinary(buf); err != nil {
		return err
	}
	return resp.Err
}

func (c *Client) ListShards(address string) (map[uint64]*meta.ShardOwnerInfo, error) {
	conn, err := c.dial(address)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Send request.
	err = WriteType(conn, listShardsRequestMessage)
	if err != nil {
		return nil, err
	}

	// Read the response.
	_, buf, err := ReadTLV(conn)
	if err != nil {
		return nil, err
	}

	// Unmarshal response.
	var resp ListShardsResponse
	if err = resp.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return resp.Shards, resp.Err
}

func (c *Client) JoinCluster(address string, metaServers []string, update bool) (*meta.NodeInfo, error) {
	conn, err := c.dial(address)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Send request.
	req := JoinClusterRequest{
		MetaServers: metaServers,
		Update:      update,
	}
	err = EncodeTLV(conn, joinClusterRequestMessage, &req)
	if err != nil {
		return nil, err
	}

	// Read the response.
	_, buf, err := ReadTLV(conn)
	if err != nil {
		return nil, err
	}

	// Unmarshal response.
	var resp JoinClusterResponse
	if err = resp.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return resp.Node, resp.Err
}

func (c *Client) LeaveCluster(address string) error {
	conn, err := c.dial(address)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send request.
	err = WriteType(conn, leaveClusterRequestMessage)
	if err != nil {
		return err
	}

	// Read the response.
	_, buf, err := ReadTLV(conn)
	if err != nil {
		return err
	}

	// Unmarshal response.
	var resp LeaveClusterResponse
	if err = resp.UnmarshalBinary(buf); err != nil {
		return err
	}
	return resp.Err
}

func (c *Client) RemoveHintedHandoff(address string, nodeID uint64) error {
	conn, err := c.dial(address)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send request.
	req := RemoveHintedHandoffRequest{
		NodeID: nodeID,
	}
	err = EncodeTLV(conn, removeHintedHandoffRequestMessage, &req)
	if err != nil {
		return err
	}

	// Read the response.
	_, buf, err := ReadTLV(conn)
	if err != nil {
		return err
	}

	// Unmarshal response.
	var resp RemoveHintedHandoffResponse
	if err = resp.UnmarshalBinary(buf); err != nil {
		return err
	}
	return resp.Err
}
