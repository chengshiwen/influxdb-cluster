package coordinator

import (
	"context"
	"io"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"golang.org/x/sync/errgroup"
)

// IteratorCreator is an interface that combines mapping fields and creating iterators.
type IteratorCreator interface {
	query.IteratorCreator
	influxql.FieldMapper
	io.Closer
}

// LocalShardMapper implements a ShardMapper for local shards.
type LocalShardMapper struct {
	MetaClient interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}

	TSDBStore interface {
		ShardGroup(ids []uint64) tsdb.ShardGroup
	}
}

// MapShards maps the sources to the appropriate shards into an IteratorCreator.
func (e *LocalShardMapper) MapShards(sources influxql.Sources, t influxql.TimeRange, opt query.SelectOptions) (query.ShardGroup, error) {
	a := &LocalShardMapping{
		ShardMap: make(map[Source]tsdb.ShardGroup),
	}

	tmin := time.Unix(0, t.MinTimeNano())
	tmax := time.Unix(0, t.MaxTimeNano())
	if err := e.mapShards(a, sources, tmin, tmax); err != nil {
		return nil, err
	}
	a.MinTime, a.MaxTime = tmin, tmax
	return a, nil
}

func (e *LocalShardMapper) mapShards(a *LocalShardMapping, sources influxql.Sources, tmin, tmax time.Time) error {
	for _, s := range sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			source := Source{
				Database:        s.Database,
				RetentionPolicy: s.RetentionPolicy,
			}
			// Retrieve the list of shards for this database. This list of
			// shards is always the same regardless of which measurement we are
			// using.
			if _, ok := a.ShardMap[source]; !ok {
				groups, err := e.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
				if err != nil {
					return err
				}

				if len(groups) == 0 {
					a.ShardMap[source] = nil
					continue
				}

				shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
				for _, g := range groups {
					for _, si := range g.Shards {
						shardIDs = append(shardIDs, si.ID)
					}
				}
				a.ShardMap[source] = e.TSDBStore.ShardGroup(shardIDs)
			}
		case *influxql.SubQuery:
			if err := e.mapShards(a, s.Statement.Sources, tmin, tmax); err != nil {
				return err
			}
		}
	}
	return nil
}

// ShardMapper maps data sources to a list of shard information.
type LocalShardMapping struct {
	ShardMap map[Source]tsdb.ShardGroup

	// MinTime is the minimum time that this shard mapper will allow.
	// Any attempt to use a time before this one will automatically result in using
	// this time instead.
	MinTime time.Time

	// MaxTime is the maximum time that this shard mapper will allow.
	// Any attempt to use a time after this one will automatically result in using
	// this time instead.
	MaxTime time.Time
}

func (a *LocalShardMapping) FieldDimensions(m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return
	}

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

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
	return
}

func (a *LocalShardMapping) MapType(m *influxql.Measurement, field string) influxql.DataType {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return influxql.Unknown
	}

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
		t := sg.MapType(name, field)
		if typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a *LocalShardMapping) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return nil, nil
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	if m.Regex != nil {
		measurements := sg.MeasurementsByRegex(m.Regex.Val)
		inputs := make([]query.Iterator, 0, len(measurements))
		if err := func() error {
			// Create a Measurement for each returned matching measurement value
			// from the regex.
			for _, measurement := range measurements {
				mm := m.Clone()
				mm.Name = measurement // Set the name to this matching regex value.
				input, err := sg.CreateIterator(ctx, mm, opt)
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

		return query.Iterators(inputs).Merge(opt)
	}
	return sg.CreateIterator(ctx, m, opt)
}

func (a *LocalShardMapping) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return query.IteratorCost{}, nil
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	if m.Regex != nil {
		var costs query.IteratorCost
		measurements := sg.MeasurementsByRegex(m.Regex.Val)
		for _, measurement := range measurements {
			cost, err := sg.IteratorCost(measurement, opt)
			if err != nil {
				return query.IteratorCost{}, err
			}
			costs = costs.Combine(cost)
		}
		return costs, nil
	}
	return sg.IteratorCost(m.Name, opt)
}

// Close clears out the list of mapped shards.
func (a *LocalShardMapping) Close() error {
	a.ShardMap = nil
	return nil
}

// ClusterShardMapper implements a ShardMapper for cluster shards.
type ClusterShardMapper struct {
	MetaClient MetaClient

	TSDBStore interface {
		ShardGroup(ids []uint64) tsdb.ShardGroup
	}

	MetaExecutor *MetaExecutor
}

// MapShards maps the sources to the appropriate shards into an IteratorCreator.
func (e *ClusterShardMapper) MapShards(sources influxql.Sources, t influxql.TimeRange, opt query.SelectOptions) (query.ShardGroup, error) {
	if opt.NodeID > 0 {
		_, err := e.MetaClient.DataNode(opt.NodeID)
		if err != nil {
			return nil, err
		}
	}

	l := &LocalShardMapping{
		ShardMap: make(map[Source]tsdb.ShardGroup),
	}
	a := &ClusterShardMapping{
		LocalShardMapping:  l,
		RemoteShardMapping: make(map[Source][]*remoteShardGroup),
		MetaExecutor:       e.MetaExecutor,
		LocalID:            e.MetaClient.NodeID(),
		NodeID:             opt.NodeID,
	}

	tmin := time.Unix(0, t.MinTimeNano())
	tmax := time.Unix(0, t.MaxTimeNano())
	if err := e.mapShards(a, sources, tmin, tmax); err != nil {
		return nil, err
	}
	l.MinTime, l.MaxTime = tmin, tmax
	a.MinTime, a.MaxTime = tmin, tmax
	return a, nil
}

func (e *ClusterShardMapper) mapShards(a *ClusterShardMapping, sources influxql.Sources, tmin, tmax time.Time) error {
	for _, s := range sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			source := Source{
				Database:        s.Database,
				RetentionPolicy: s.RetentionPolicy,
			}
			// Retrieve the list of shards for this database. This list of
			// shards is always the same regardless of which measurement we are
			// using.
			if _, ok := a.RemoteShardMapping[source]; !ok {
				groups, err := e.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
				if err != nil {
					return err
				}

				if len(groups) == 0 {
					a.LocalShardMapping.ShardMap[source] = nil
					a.RemoteShardMapping[source] = nil
					continue
				}

				// Map shards to nodes.
				shardsByNodeID := make(map[uint64]shardInfos)
				if a.NodeID > 0 {
					// Node to exclusively read from.
					for _, g := range groups {
						for _, si := range g.Shards {
							if si.OwnedBy(a.NodeID) {
								if _, ok := shardsByNodeID[a.NodeID]; !ok {
									shardsByNodeID[a.NodeID] = make(shardInfos, 0, len(groups))
								}
								shardsByNodeID[a.NodeID] = append(shardsByNodeID[a.NodeID], si)
							}
						}
					}
				} else {
					// If zero, all nodes are used.
					for _, g := range groups {
						for _, si := range g.Shards {
							// Always assign to local node if it has the shard.
							// Otherwise randomly select a remote node.
							var nodeID uint64
							if si.OwnedBy(a.LocalID) {
								nodeID = a.LocalID
							} else if len(si.Owners) > 0 {
								// The selected node has higher priority.
								for _, owner := range si.Owners {
									if _, ok := shardsByNodeID[owner.NodeID]; ok {
										nodeID = owner.NodeID
										break
									}
								}
								// Otherwise randomly select.
								if nodeID == 0 {
									nodeID = si.Owners[rand.Intn(len(si.Owners))].NodeID
								}
							} else {
								// This should not occur but if the shard has no owners then
								// we don't want this to panic by trying to randomly select a node.
								continue
							}
							if _, ok := shardsByNodeID[nodeID]; !ok {
								shardsByNodeID[nodeID] = make(shardInfos, 0, len(groups))
							}
							shardsByNodeID[nodeID] = append(shardsByNodeID[nodeID], si)
						}
					}
				}

				// Generate shard map for each node.
				for nodeID, shards := range shardsByNodeID {
					// Record local shard id if local.
					if nodeID == a.LocalID {
						a.LocalShardMapping.ShardMap[source] = e.TSDBStore.ShardGroup(shards.shardIDs())
						continue
					}

					// Otherwise create shard group remotely.
					retry := nodeID != a.NodeID
					shardGroup := newRemoteShardGroup(a.MetaExecutor, nodeID, shards, retry)
					if _, ok := a.RemoteShardMapping[source]; !ok {
						a.RemoteShardMapping[source] = make([]*remoteShardGroup, 0, len(shardsByNodeID))
					}
					a.RemoteShardMapping[source] = append(a.RemoteShardMapping[source], shardGroup)
				}
			}
		case *influxql.SubQuery:
			if err := e.mapShards(a, s.Statement.Sources, tmin, tmax); err != nil {
				return err
			}
		}
	}
	return nil
}

// ClusterShardMapping maps data sources to a list of shard information.
type ClusterShardMapping struct {
	LocalShardMapping *LocalShardMapping

	RemoteShardMapping map[Source][]*remoteShardGroup

	MetaExecutor *MetaExecutor

	// MinTime is the minimum time that this shard mapper will allow.
	// Any attempt to use a time before this one will automatically result in using
	// this time instead.
	MinTime time.Time

	// MaxTime is the maximum time that this shard mapper will allow.
	// Any attempt to use a time after this one will automatically result in using
	// this time instead.
	MaxTime time.Time

	// Node to query locally.
	LocalID uint64

	// Node to execute on.
	NodeID uint64
}

func (a *ClusterShardMapping) FieldDimensions(m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	var mu sync.Mutex
	var g errgroup.Group
	entries := make([]FieldDimensionsEntry, 0, len(a.RemoteShardMapping[source])+1)

	g.Go(func() error {
		f, d, err := a.LocalShardMapping.FieldDimensions(m)
		if err != nil {
			return err
		}
		entry := FieldDimensionsEntry{Fields: f, Dimensions: d}
		mu.Lock()
		entries = append(entries, entry)
		mu.Unlock()
		return nil
	})

	for _, sg := range a.RemoteShardMapping[source] {
		sg := sg
		g.Go(func() error {
			results, err := sg.FieldDimensions(m)
			if err != nil {
				return err
			}
			if len(results) > 0 {
				mu.Lock()
				entries = append(entries, results...)
				mu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})
	for _, e := range entries {
		for k, typ := range e.Fields {
			fields[k] = typ
		}
		for k := range e.Dimensions {
			dimensions[k] = struct{}{}
		}
	}
	return
}

func (a *ClusterShardMapping) MapType(m *influxql.Measurement, field string) influxql.DataType {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	var mu sync.Mutex
	var g errgroup.Group
	types := make([]influxql.DataType, 0, len(a.RemoteShardMapping[source])+1)

	g.Go(func() error {
		typ := a.LocalShardMapping.MapType(m, field)
		mu.Lock()
		types = append(types, typ)
		mu.Unlock()
		return nil
	})

	for _, sg := range a.RemoteShardMapping[source] {
		sg := sg
		g.Go(func() error {
			results := sg.MapType(m, field)
			if len(results) > 0 {
				mu.Lock()
				types = append(types, results...)
				mu.Unlock()
			}
			return nil
		})
	}

	g.Wait()

	var typ influxql.DataType
	for _, t := range types {
		if typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a *ClusterShardMapping) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	var mu sync.Mutex
	var g errgroup.Group
	inputs := make([]query.Iterator, 0, len(a.RemoteShardMapping[source])+1)

	g.Go(func() error {
		input, err := a.LocalShardMapping.CreateIterator(ctx, m, opt)
		if err != nil {
			return err
		}
		if input != nil {
			mu.Lock()
			inputs = append(inputs, input)
			mu.Unlock()
		}
		return nil
	})

	for _, sg := range a.RemoteShardMapping[source] {
		sg := sg
		g.Go(func() error {
			results, err := sg.CreateIterator(ctx, m, opt)
			if err != nil {
				return err
			}
			if len(results) > 0 {
				mu.Lock()
				inputs = append(inputs, results...)
				mu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		query.Iterators(inputs).Close()
		return nil, err
	}

	return query.Iterators(inputs).Merge(opt)
}

func (a *ClusterShardMapping) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	var mu sync.Mutex
	var g errgroup.Group
	costs := make([]query.IteratorCost, 0, len(a.RemoteShardMapping[source])+1)

	g.Go(func() error {
		cost, err := a.LocalShardMapping.IteratorCost(m, opt)
		if err != nil {
			return err
		}
		mu.Lock()
		costs = append(costs, cost)
		mu.Unlock()
		return nil
	})

	for _, sg := range a.RemoteShardMapping[source] {
		sg := sg
		g.Go(func() error {
			results, err := sg.IteratorCost(m, opt)
			if err != nil {
				return err
			}
			if len(results) > 0 {
				mu.Lock()
				costs = append(costs, results...)
				mu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return query.IteratorCost{}, err
	}

	var cost query.IteratorCost
	for _, c := range costs {
		cost = cost.Combine(c)
	}
	return cost, nil
}

// Close clears out the list of mapped shards.
func (a *ClusterShardMapping) Close() error {
	a.LocalShardMapping.Close()
	for _, sgs := range a.RemoteShardMapping {
		for _, sg := range sgs {
			sg.Close()
		}
	}
	a.RemoteShardMapping = nil
	return nil
}

// Store describes the behaviour of the storage packages Store type.
type Store interface {
	ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error)
	ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error)
}

// ClusterStoreMapper implements a StoreMapper for cluster store.
type ClusterStoreMapper struct {
	MetaClient interface {
		NodeID() uint64
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}

	MetaExecutor *MetaExecutor
}

// MapShards maps the sources to the appropriate shards into an Store.
func (e *ClusterStoreMapper) MapShards(database, rp string, start, end int64, nodeID uint64, cursorFn func(ctx context.Context, shardIDs []uint64) (reads.SeriesCursor, error)) (Store, error) {
	groups, err := e.MetaClient.ShardGroupsByTimeRange(database, rp, time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return nil, err
	}

	if len(groups) == 0 {
		return nil, nil
	}

	a := &ClusterStoreMapping{
		CursorFn: cursorFn,
		LocalID:  e.MetaClient.NodeID(),
		NodeID:   nodeID,
	}

	if err := e.mapShards(a, groups); err != nil {
		return nil, err
	}
	return a, nil
}

func (e *ClusterStoreMapper) mapShards(a *ClusterStoreMapping, groups []meta.ShardGroupInfo) error {
	// Map shards to nodes.
	shardsByNodeID := make(map[uint64]shardInfos)
	if a.NodeID > 0 {
		// Node to exclusively read from.
		for _, g := range groups {
			for _, si := range g.Shards {
				if si.OwnedBy(a.NodeID) {
					if _, ok := shardsByNodeID[a.NodeID]; !ok {
						shardsByNodeID[a.NodeID] = make(shardInfos, 0, len(groups))
					}
					shardsByNodeID[a.NodeID] = append(shardsByNodeID[a.NodeID], si)
				}
			}
		}
	} else {
		// If zero, all nodes are used.
		for _, g := range groups {
			for _, si := range g.Shards {
				// Always assign to local node if it has the shard.
				// Otherwise randomly select a remote node.
				var nodeID uint64
				if si.OwnedBy(a.LocalID) {
					nodeID = a.LocalID
				} else if len(si.Owners) > 0 {
					// The selected node has higher priority.
					for _, owner := range si.Owners {
						if _, ok := shardsByNodeID[owner.NodeID]; ok {
							nodeID = owner.NodeID
							break
						}
					}
					// Otherwise randomly select.
					if nodeID == 0 {
						nodeID = si.Owners[rand.Intn(len(si.Owners))].NodeID
					}
				} else {
					// This should not occur but if the shard has no owners then
					// we don't want this to panic by trying to randomly select a node.
					continue
				}
				if _, ok := shardsByNodeID[nodeID]; !ok {
					shardsByNodeID[nodeID] = make(shardInfos, 0, len(groups))
				}
				shardsByNodeID[nodeID] = append(shardsByNodeID[nodeID], si)
			}
		}
	}

	// Generate shard map for each node.
	for nodeID, shards := range shardsByNodeID {
		// Record local shard id if local.
		if nodeID == a.LocalID {
			a.LocalShardIDs = shards.shardIDs()
			continue
		}

		// Otherwise create shard group remotely.
		retry := nodeID != a.NodeID
		shardGroup := newRemoteShardGroup(e.MetaExecutor, nodeID, shards, retry)
		a.RemoteShardGroups = append(a.RemoteShardGroups, shardGroup)
	}
	return nil
}

// ClusterStoreMapping maps nodes to a list of shard information.
type ClusterStoreMapping struct {
	LocalShardIDs []uint64

	RemoteShardGroups []*remoteShardGroup

	CursorFn func(ctx context.Context, shardIDs []uint64) (reads.SeriesCursor, error)

	// Node to query locally.
	LocalID uint64

	// Node to execute on.
	NodeID uint64
}

func (a *ClusterStoreMapping) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	if len(a.LocalShardIDs) == 0 && len(a.RemoteShardGroups) == 0 {
		return nil, nil
	}

	var mu sync.Mutex
	var g errgroup.Group
	rss := make([]reads.ResultSet, 0, len(a.RemoteShardGroups)+1)

	g.Go(func() error {
		if len(a.LocalShardIDs) == 0 || a.CursorFn == nil {
			return nil
		}
		cur, err := a.CursorFn(ctx, a.LocalShardIDs)
		if err != nil {
			return err
		} else if cur == nil {
			return nil
		}

		rs := reads.NewFilteredResultSet(ctx, req, cur)
		mu.Lock()
		rss = append(rss, rs)
		mu.Unlock()
		return nil
	})

	for _, sg := range a.RemoteShardGroups {
		sg := sg
		g.Go(func() error {
			results, err := sg.ReadFilter(ctx, req)
			if err != nil {
				return err
			}
			if len(results) > 0 {
				mu.Lock()
				rss = append(rss, results...)
				mu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		for _, rs := range rss {
			rs.Close()
		}
		return nil, err
	}

	return reads.NewMergedResultSet(rss), nil
}

func (a *ClusterStoreMapping) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	if len(a.LocalShardIDs) == 0 && len(a.RemoteShardGroups) == 0 {
		return nil, nil
	}

	var mu sync.Mutex
	var g errgroup.Group
	rss := make([]reads.GroupResultSet, 0, len(a.RemoteShardGroups)+1)

	g.Go(func() error {
		if len(a.LocalShardIDs) == 0 || a.CursorFn == nil {
			return nil
		}
		newCursor := func() (reads.SeriesCursor, error) {
			return a.CursorFn(ctx, a.LocalShardIDs)
		}

		rs := reads.NewGroupResultSet(ctx, req, newCursor)
		if rs == nil {
			return nil
		}
		mu.Lock()
		rss = append(rss, rs)
		mu.Unlock()
		return nil
	})

	for _, sg := range a.RemoteShardGroups {
		sg := sg
		g.Go(func() error {
			results, err := sg.ReadGroup(ctx, req)
			if err != nil {
				return err
			}
			if len(results) > 0 {
				mu.Lock()
				rss = append(rss, results...)
				mu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		for _, rs := range rss {
			rs.Close()
		}
		return nil, err
	}

	switch req.Group {
	case datatypes.GroupBy:
		return reads.NewGroupByMergedGroupResultSet(rss), nil
	case datatypes.GroupNone:
		return reads.NewGroupNoneMergedGroupResultSet(rss), nil
	default:
		panic("not implemented")
	}
}

// remoteShardGroup creates shard groups for remote shards.
type remoteShardGroup struct {
	executor *MetaExecutor
	nodeID   uint64
	shards   shardInfos
	retry    bool
	dirty    sync.Map
}

// newRemoteShardGroup returns a new instance of newRemoteShardGroup for remote shards.
func newRemoteShardGroup(executor *MetaExecutor, nodeID uint64, shards shardInfos, retry bool) *remoteShardGroup {
	return &remoteShardGroup{
		executor: executor,
		nodeID:   nodeID,
		shards:   shards,
		retry:    retry,
	}
}

func (a *remoteShardGroup) shuffleShards() map[uint64]shardInfos {
	var shardsByNodeID map[uint64]shardInfos
	for _, si := range a.shards {
		if len(si.Owners) == 0 {
			continue
		}
		var nodeID uint64
		if len(shardsByNodeID) > 0 {
			for _, owner := range si.Owners {
				if _, ok := shardsByNodeID[owner.NodeID]; ok {
					nodeID = owner.NodeID
					break
				}
			}
		}
		if nodeID == 0 {
			for _, owner := range si.Owners {
				if _, ok := a.dirty.Load(owner.NodeID); !ok {
					nodeID = owner.NodeID
					break
				}
			}
		}
		if nodeID == 0 {
			return nil
		}
		if shardsByNodeID == nil {
			shardsByNodeID = make(map[uint64]shardInfos)
		}
		if _, ok := shardsByNodeID[nodeID]; !ok {
			shardsByNodeID[nodeID] = make(shardInfos, 0, len(a.shards))
		}
		shardsByNodeID[nodeID] = append(shardsByNodeID[nodeID], si)
	}
	return shardsByNodeID
}

func (a *remoteShardGroup) FieldDimensions(m *influxql.Measurement) ([]FieldDimensionsEntry, error) {
	f, d, err := a.executor.FieldDimensions(a.nodeID, a.shards.shardIDs(), m)
	if err == nil {
		return []FieldDimensionsEntry{{Fields: f, Dimensions: d}}, nil
	}
	if !a.retry {
		return nil, err
	}
	a.dirty.Store(a.nodeID, struct{}{})
	for shardsByNodeID := a.shuffleShards(); shardsByNodeID != nil; shardsByNodeID = a.shuffleShards() {
		var mu sync.Mutex
		var g errgroup.Group
		entries := make([]FieldDimensionsEntry, 0, len(shardsByNodeID))
		for nodeID, shards := range shardsByNodeID {
			nodeID, shards := nodeID, shards
			g.Go(func() error {
				f, d, err := a.executor.FieldDimensions(nodeID, shards.shardIDs(), m)
				if err != nil {
					a.dirty.Store(nodeID, struct{}{})
					return err
				}
				entry := FieldDimensionsEntry{Fields: f, Dimensions: d}
				mu.Lock()
				entries = append(entries, entry)
				mu.Unlock()
				return nil
			})
		}
		err = g.Wait()
		if err == nil {
			return entries, nil
		}
	}
	return nil, err
}

func (a *remoteShardGroup) MapType(m *influxql.Measurement, field string) []influxql.DataType {
	typ, err := a.executor.MapType(a.nodeID, a.shards.shardIDs(), m, field)
	if err == nil {
		return []influxql.DataType{typ}
	}
	if !a.retry {
		return nil
	}
	var types []influxql.DataType
	a.dirty.Store(a.nodeID, struct{}{})
	for shardsByNodeID := a.shuffleShards(); shardsByNodeID != nil; shardsByNodeID = a.shuffleShards() {
		var mu sync.Mutex
		var g errgroup.Group
		types = make([]influxql.DataType, 0, len(shardsByNodeID))
		for nodeID, shards := range shardsByNodeID {
			nodeID, shards := nodeID, shards
			g.Go(func() error {
				typ, err := a.executor.MapType(nodeID, shards.shardIDs(), m, field)
				if err != nil {
					a.dirty.Store(nodeID, struct{}{})
					return err
				}
				mu.Lock()
				types = append(types, typ)
				mu.Unlock()
				return nil
			})
		}
		err = g.Wait()
		if err == nil {
			return types
		}
	}
	return types
}

func (a *remoteShardGroup) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) ([]query.Iterator, error) {
	input, err := a.executor.CreateIterator(a.nodeID, a.shards.shardIDs(), ctx, m, opt)
	if err == nil {
		return []query.Iterator{input}, nil
	}
	if !a.retry {
		return nil, err
	}
	a.dirty.Store(a.nodeID, struct{}{})
	for shardsByNodeID := a.shuffleShards(); shardsByNodeID != nil; shardsByNodeID = a.shuffleShards() {
		var mu sync.Mutex
		var g errgroup.Group
		inputs := make([]query.Iterator, 0, len(shardsByNodeID))
		for nodeID, shards := range shardsByNodeID {
			nodeID, shards := nodeID, shards
			g.Go(func() error {
				input, err := a.executor.CreateIterator(nodeID, shards.shardIDs(), ctx, m, opt)
				if err != nil {
					a.dirty.Store(nodeID, struct{}{})
					return err
				}
				mu.Lock()
				inputs = append(inputs, input)
				mu.Unlock()
				return nil
			})
		}
		err = g.Wait()
		if err == nil {
			return inputs, nil
		}
		query.Iterators(inputs).Close()
	}
	return nil, err
}

func (a *remoteShardGroup) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions) ([]query.IteratorCost, error) {
	cost, err := a.executor.IteratorCost(a.nodeID, a.shards.shardIDs(), m, opt)
	if err == nil {
		return []query.IteratorCost{cost}, nil
	}
	if !a.retry {
		return nil, err
	}
	a.dirty.Store(a.nodeID, struct{}{})
	for shardsByNodeID := a.shuffleShards(); shardsByNodeID != nil; shardsByNodeID = a.shuffleShards() {
		var mu sync.Mutex
		var g errgroup.Group
		costs := make([]query.IteratorCost, 0, len(shardsByNodeID))
		for nodeID, shards := range shardsByNodeID {
			nodeID, shards := nodeID, shards
			g.Go(func() error {
				cost, err := a.executor.IteratorCost(nodeID, shards.shardIDs(), m, opt)
				if err != nil {
					a.dirty.Store(nodeID, struct{}{})
					return err
				}
				mu.Lock()
				costs = append(costs, cost)
				mu.Unlock()
				return nil
			})
		}
		err = g.Wait()
		if err == nil {
			return costs, nil
		}
	}
	return nil, err
}

func (a *remoteShardGroup) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) ([]reads.ResultSet, error) {
	rs, err := a.executor.ReadFilter(a.nodeID, a.shards.shardIDs(), ctx, req)
	if err == nil {
		return []reads.ResultSet{rs}, nil
	}
	if !a.retry {
		return nil, err
	}
	a.dirty.Store(a.nodeID, struct{}{})
	for shardsByNodeID := a.shuffleShards(); shardsByNodeID != nil; shardsByNodeID = a.shuffleShards() {
		var mu sync.Mutex
		var g errgroup.Group
		rss := make([]reads.ResultSet, 0, len(shardsByNodeID))
		for nodeID, shards := range shardsByNodeID {
			nodeID, shards := nodeID, shards
			g.Go(func() error {
				input, err := a.executor.ReadFilter(nodeID, shards.shardIDs(), ctx, req)
				if err != nil {
					a.dirty.Store(nodeID, struct{}{})
					return err
				}
				mu.Lock()
				rss = append(rss, input)
				mu.Unlock()
				return nil
			})
		}
		err = g.Wait()
		if err == nil {
			return rss, nil
		}
		for _, rs := range rss {
			rs.Close()
		}
	}
	return nil, err
}

func (a *remoteShardGroup) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) ([]reads.GroupResultSet, error) {
	rs, err := a.executor.ReadGroup(a.nodeID, a.shards.shardIDs(), ctx, req)
	if err == nil {
		return []reads.GroupResultSet{rs}, nil
	}
	if !a.retry {
		return nil, err
	}
	a.dirty.Store(a.nodeID, struct{}{})
	for shardsByNodeID := a.shuffleShards(); shardsByNodeID != nil; shardsByNodeID = a.shuffleShards() {
		var mu sync.Mutex
		var g errgroup.Group
		rss := make([]reads.GroupResultSet, 0, len(shardsByNodeID))
		for nodeID, shards := range shardsByNodeID {
			nodeID, shards := nodeID, shards
			g.Go(func() error {
				input, err := a.executor.ReadGroup(nodeID, shards.shardIDs(), ctx, req)
				if err != nil {
					a.dirty.Store(nodeID, struct{}{})
					return err
				}
				mu.Lock()
				rss = append(rss, input)
				mu.Unlock()
				return nil
			})
		}
		err = g.Wait()
		if err == nil {
			return rss, nil
		}
		for _, rs := range rss {
			rs.Close()
		}
	}
	return nil, err
}

func (a *remoteShardGroup) Close() error {
	a.shards = nil
	return nil
}

type FieldDimensionsEntry struct {
	Fields     map[string]influxql.DataType
	Dimensions map[string]struct{}
}

type shardInfos []meta.ShardInfo

func (a shardInfos) shardIDs() []uint64 {
	var shardIDs []uint64
	if len(a) > 0 {
		shardIDs = make([]uint64, 0, len(a))
	}
	for _, si := range a {
		shardIDs = append(shardIDs, si.ID)
	}
	// Sort shard IDs so we get more predicable execution.
	sort.Sort(uint64Slice(shardIDs))
	return shardIDs
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }

// Source contains the database and retention policy source for data.
type Source struct {
	Database        string
	RetentionPolicy string
}
