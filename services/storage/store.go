package storage

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

var (
	ErrMissingReadSource = errors.New("missing ReadSource")
)

// GetReadSource will attempt to unmarshal a ReadSource from the ReadRequest or
// return an error if no valid resource is present.
func GetReadSource(any types.Any) (*ReadSource, error) {
	var source ReadSource
	if err := types.UnmarshalAny(&any, &source); err != nil {
		return nil, err
	}
	return &source, nil
}

type TSDBStore interface {
	Shards(ids []uint64) []*tsdb.Shard
	MeasurementNames(ctx context.Context, auth query.FineAuthorizer, database string, retentionPolicy string, cond influxql.Expr) ([][]byte, error)
	TagKeys(ctx context.Context, auth query.FineAuthorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValues(ctx context.Context, auth query.FineAuthorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
}

type MetaClient interface {
	NodeID() uint64
	Database(name string) *meta.DatabaseInfo
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
}

type Store struct {
	TSDBStore  TSDBStore
	MetaClient MetaClient
	Logger     *zap.Logger
}

func NewStore(store TSDBStore, metaClient MetaClient) *Store {
	return &Store{
		TSDBStore:  store,
		MetaClient: metaClient,
		Logger:     zap.NewNop(),
	}
}

// WithLogger sets the logger for the service.
func (s *Store) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "store"))
}

func (s *Store) findShardIDs(database, rp string, desc bool, start, end int64) ([]uint64, error) {
	groups, err := s.MetaClient.ShardGroupsByTimeRange(database, rp, time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return nil, err
	}

	if len(groups) == 0 {
		return nil, nil
	}

	if desc {
		sort.Sort(sort.Reverse(meta.ShardGroupInfos(groups)))
	} else {
		sort.Sort(meta.ShardGroupInfos(groups))
	}

	shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
	for _, g := range groups {
		for _, si := range g.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}
	return shardIDs, nil
}

func (s *Store) validateArgs(database, rp string, start, end int64) (string, string, int64, int64, error) {
	di := s.MetaClient.Database(database)
	if di == nil {
		return "", "", 0, 0, errors.New("no database")
	}

	if rp == "" {
		rp = di.DefaultRetentionPolicy
	}

	rpi := di.RetentionPolicy(rp)
	if rpi == nil {
		return "", "", 0, 0, errors.New("invalid retention policy")
	}

	if start <= models.MinNanoTime {
		start = models.MinNanoTime
	}
	if end >= models.MaxNanoTime {
		end = models.MaxNanoTime
	}
	return database, rp, start, end, nil
}

func (s *Store) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.Range.Start, req.Range.End)
	if err != nil {
		return nil, err
	}

	var shardIDs []uint64
	if sIDs, ok := ctx.Value(coordinator.ShardIDsKey).([]uint64); ok {
		shardIDs = sIDs
	} else {
		shardIDs, err = s.findShardIDs(database, rp, false, start, end)
		if err != nil {
			return nil, err
		}
	}
	if len(shardIDs) == 0 { // TODO(jeff): this was a typed nil
		return nil, nil
	}

	var cur reads.SeriesCursor
	if ic, err := newIndexSeriesCursor(ctx, req.Predicate, s.TSDBStore.Shards(shardIDs)); err != nil {
		return nil, err
	} else if ic == nil { // TODO(jeff): this was a typed nil
		return nil, nil
	} else {
		cur = ic
	}

	req.Range.Start = start
	req.Range.End = end

	return reads.NewFilteredResultSet(ctx, req, cur), nil
}

func (s *Store) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.Range.Start, req.Range.End)
	if err != nil {
		return nil, err
	}

	var shardIDs []uint64
	if sIDs, ok := ctx.Value(coordinator.ShardIDsKey).([]uint64); ok {
		shardIDs = sIDs
	} else {
		shardIDs, err = s.findShardIDs(database, rp, false, start, end)
		if err != nil {
			return nil, err
		}
	}
	if len(shardIDs) == 0 {
		return nil, nil
	}

	shards := s.TSDBStore.Shards(shardIDs)

	req.Range.Start = start
	req.Range.End = end

	newCursor := func() (reads.SeriesCursor, error) {
		cur, err := newIndexSeriesCursor(ctx, req.Predicate, shards)
		if cur == nil || err != nil {
			return nil, err
		}
		return cur, nil
	}

	rs := reads.NewGroupResultSet(ctx, req, newCursor)
	if rs == nil {
		return nil, nil
	}

	return rs, nil
}

func (s *Store) TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
	if req.TagsSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(*req.TagsSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.Range.Start, req.Range.End)
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(database, rp, false, start, end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 {
		return cursors.NewStringSliceIterator(nil), nil
	}

	var expr influxql.Expr
	if root := req.Predicate.GetRoot(); root != nil {
		var err error
		expr, err = reads.NodeToExpr(root, measurementRemap)
		if err != nil {
			return nil, err
		}

		if found := reads.HasFieldValueKey(expr); found {
			return nil, errors.New("field values unsupported")
		}
		expr = influxql.Reduce(influxql.CloneExpr(expr), nil)
		if reads.IsTrueBooleanLiteral(expr) {
			expr = nil
		}
	}

	// TODO(jsternberg): Use a real authorizer.
	auth := query.OpenAuthorizer
	keys, err := s.TSDBStore.TagKeys(ctx, auth, shardIDs, expr)
	if err != nil {
		return nil, err
	}

	m := map[string]bool{
		measurementKey: true,
		fieldKey:       true,
	}
	for _, ks := range keys {
		for _, k := range ks.Keys {
			m[k] = true
		}
	}

	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return cursors.NewStringSliceIterator(names), nil
}

func (s *Store) TagValues(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error) {
	if tagKey, ok := measurementRemap[req.TagKey]; ok {
		switch tagKey {
		case "_name":
			return s.MeasurementNames(ctx, &MeasurementNamesRequest{
				MeasurementsSource: req.TagsSource,
				Predicate:          req.Predicate,
			})
		}
	}

	if req.TagsSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(*req.TagsSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.Range.Start, req.Range.End)
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(database, rp, false, start, end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 {
		return cursors.NewStringSliceIterator(nil), nil
	}

	var expr influxql.Expr
	if root := req.Predicate.GetRoot(); root != nil {
		var err error
		expr, err = reads.NodeToExpr(root, measurementRemap)
		if err != nil {
			return nil, err
		}

		if found := reads.HasFieldValueKey(expr); found {
			return nil, errors.New("field values unsupported")
		}
		expr = influxql.Reduce(influxql.CloneExpr(expr), nil)
		if reads.IsTrueBooleanLiteral(expr) {
			expr = nil
		}
	}

	tagKeyExpr := &influxql.BinaryExpr{
		Op: influxql.EQ,
		LHS: &influxql.VarRef{
			Val: "_tagKey",
		},
		RHS: &influxql.StringLiteral{
			Val: req.TagKey,
		},
	}
	if expr != nil {
		expr = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: tagKeyExpr,
			RHS: &influxql.ParenExpr{
				Expr: expr,
			},
		}
	} else {
		expr = tagKeyExpr
	}

	// TODO(jsternberg): Use a real authorizer.
	auth := query.OpenAuthorizer
	values, err := s.TSDBStore.TagValues(ctx, auth, shardIDs, expr)
	if err != nil {
		return nil, err
	}

	m := make(map[string]struct{})
	for _, kvs := range values {
		for _, kv := range kvs.Values {
			m[kv.Value] = struct{}{}
		}
	}

	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return cursors.NewStringSliceIterator(names), nil
}

type MeasurementNamesRequest struct {
	MeasurementsSource *types.Any
	Predicate          *datatypes.Predicate
}

func (s *Store) MeasurementNames(ctx context.Context, req *MeasurementNamesRequest) (cursors.StringIterator, error) {
	if req.MeasurementsSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(*req.MeasurementsSource)
	if err != nil {
		return nil, err
	}

	database, _, _, _, err := s.validateArgs(source.Database, source.RetentionPolicy, -1, -1)
	if err != nil {
		return nil, err
	}

	var expr influxql.Expr
	if root := req.Predicate.GetRoot(); root != nil {
		var err error
		expr, err = reads.NodeToExpr(root, nil)
		if err != nil {
			return nil, err
		}

		if found := reads.HasFieldValueKey(expr); found {
			return nil, errors.New("field values unsupported")
		}
		expr = influxql.Reduce(influxql.CloneExpr(expr), nil)
		if reads.IsTrueBooleanLiteral(expr) {
			expr = nil
		}
	}

	// TODO(jsternberg): Use a real authorizer.
	auth := query.OpenAuthorizer
	// NOTE: this preserves the existing flux behaviour of ignoring the retention policy here
	values, err := s.TSDBStore.MeasurementNames(ctx, auth, database, "", expr)
	if err != nil {
		return nil, err
	}

	m := make(map[string]struct{})
	for _, name := range values {
		m[string(name)] = struct{}{}
	}

	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return cursors.NewStringSliceIterator(names), nil
}

func (s *Store) GetSource(db, rp string) proto.Message {
	return &ReadSource{Database: db, RetentionPolicy: rp}
}

type StoreMapper interface {
	MapShards(database, rp string, start, end int64, nodeID uint64, cursorFn func(ctx context.Context, shardIDs []uint64) (reads.SeriesCursor, error)) (coordinator.Store, error)
}

type ClusterStore struct {
	*Store

	StoreMapper StoreMapper
}

func NewClusterStore(store TSDBStore, metaClient MetaClient, metaExecutor *coordinator.MetaExecutor) *ClusterStore {
	return &ClusterStore{
		Store:       NewStore(store, metaClient),
		StoreMapper: &coordinator.ClusterStoreMapper{MetaClient: metaClient, MetaExecutor: metaExecutor},
	}
}

func (s *ClusterStore) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.Range.Start, req.Range.End)
	if err != nil {
		return nil, err
	}

	var nodeID uint64
	if opts := ReadOptionsFromContext(ctx); opts != nil {
		nodeID = opts.NodeID
	}

	req.Range.Start = start
	req.Range.End = end

	cursorFn := func(ctx context.Context, shardIDs []uint64) (reads.SeriesCursor, error) {
		var cur reads.SeriesCursor
		if ic, err := newIndexSeriesCursor(ctx, req.Predicate, s.TSDBStore.Shards(shardIDs)); err != nil {
			return nil, err
		} else if ic == nil { // TODO(jeff): this was a typed nil
			return nil, nil
		} else {
			cur = ic
		}
		return cur, nil
	}

	shards, err := s.StoreMapper.MapShards(database, rp, start, end, nodeID, cursorFn)
	if err != nil {
		return nil, err
	} else if shards == nil {
		return nil, nil
	}

	return shards.ReadFilter(ctx, req)
}

func (s *ClusterStore) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := GetReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.Range.Start, req.Range.End)
	if err != nil {
		return nil, err
	}

	var nodeID uint64
	if opts := ReadOptionsFromContext(ctx); opts != nil {
		nodeID = opts.NodeID
	}

	req.Range.Start = start
	req.Range.End = end

	cursorFn := func(ctx context.Context, shardIDs []uint64) (reads.SeriesCursor, error) {
		cur, err := newIndexSeriesCursor(ctx, req.Predicate, s.TSDBStore.Shards(shardIDs))
		if cur == nil || err != nil {
			return nil, err
		}
		return cur, nil
	}

	shards, err := s.StoreMapper.MapShards(database, rp, start, end, nodeID, cursorFn)
	if err != nil {
		return nil, err
	} else if shards == nil {
		return nil, nil
	}

	return shards.ReadGroup(ctx, req)
}
