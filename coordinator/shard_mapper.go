package coordinator

import (
	"context"
	"io"
	"math/rand"
	"net"
	"sort"
	"time"

	"github.com/influxdata/influxdb/coordinator/rpc"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
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
				inputs = append(inputs, input)
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
		LocalShardMapping: l,
		RemoteShardMap:    make(map[Source][]*remoteIteratorCreator),
	}

	tmin := time.Unix(0, t.MinTimeNano())
	tmax := time.Unix(0, t.MaxTimeNano())
	if err := e.mapShards(a, sources, tmin, tmax); err != nil {
		return nil, err
	}
	l.MinTime, l.MaxTime = tmin, tmax
	a.MinTime, a.MaxTime = tmin, tmax
	a.NodeID = opt.NodeID
	return a, nil
}

func (e *ClusterShardMapper) mapShards(a *ClusterShardMapping, sources influxql.Sources, tmin, tmax time.Time) error {
	localNodeID := e.MetaClient.NodeID()
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
			if _, ok := a.RemoteShardMap[source]; !ok {
				groups, err := e.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
				if err != nil {
					return err
				}

				if len(groups) == 0 {
					a.LocalShardMapping.ShardMap[source] = nil
					a.RemoteShardMap[source] = nil
					continue
				}

				// Map shards to nodes.
				shardIDsByNodeID := make(map[uint64][]uint64, len(groups[0].Shards)*len(groups))
				if a.NodeID > 0 {
					// Node to exclusively read from.
					for _, g := range groups {
						for _, si := range g.Shards {
							if si.OwnedBy(a.NodeID) {
								shardIDsByNodeID[a.NodeID] = append(shardIDsByNodeID[a.NodeID], si.ID)
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
							if si.OwnedBy(localNodeID) {
								nodeID = localNodeID
							} else if len(si.Owners) > 0 {
								nodeID = si.Owners[rand.Intn(len(si.Owners))].NodeID
							} else {
								// This should not occur but if the shard has no owners then
								// we don't want this to panic by trying to randomly select a node.
								continue
							}
							shardIDsByNodeID[nodeID] = append(shardIDsByNodeID[nodeID], si.ID)
						}
					}
				}

				// Generate shard map for each node.
				for nodeID, shardIDs := range shardIDsByNodeID {
					// Sort shard IDs so we get more predicable execution.
					sort.Sort(uint64Slice(shardIDs))

					// Record local shard id if local.
					if nodeID == localNodeID {
						a.LocalShardMapping.ShardMap[source] = e.TSDBStore.ShardGroup(shardIDs)
						continue
					}

					// Otherwise create iterator creator remotely.
					dialer := &NodeDialer{
						MetaClient: e.MetaClient,
						Timeout:    DefaultShardMapperTimeout,
					}
					remoteIC := newRemoteIteratorCreator(dialer, nodeID, shardIDs, a.MinTime, a.MaxTime)
					if _, ok := a.RemoteShardMap[source]; !ok {
						a.RemoteShardMap[source] = make([]*remoteIteratorCreator, 0, len(shardIDsByNodeID))
					}
					a.RemoteShardMap[source] = append(a.RemoteShardMap[source], remoteIC)
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

	RemoteShardMap map[Source][]*remoteIteratorCreator

	// MinTime is the minimum time that this shard mapper will allow.
	// Any attempt to use a time before this one will automatically result in using
	// this time instead.
	MinTime time.Time

	// MaxTime is the maximum time that this shard mapper will allow.
	// Any attempt to use a time after this one will automatically result in using
	// this time instead.
	MaxTime time.Time

	// Node to execute on.
	NodeID uint64
}

func (a *ClusterShardMapping) FieldDimensions(m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	f, d, err := a.LocalShardMapping.FieldDimensions(m)
	if err != nil {
		return nil, nil, err
	}

	for k, typ := range f {
		fields[k] = typ
	}
	for k := range d {
		dimensions[k] = struct{}{}
	}

	for _, ic := range a.RemoteShardMap[source] {
		f, d, err := ic.FieldDimensions(m)
		if err != nil {
			return nil, nil, err
		}
		for k, typ := range f {
			fields[k] = typ
		}
		for k := range d {
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

	typ := a.LocalShardMapping.MapType(m, field)
	for _, ic := range a.RemoteShardMap[source] {
		t := ic.MapType(m, field)
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

	inputs := make([]query.Iterator, 0, len(a.RemoteShardMap[source])+1)
	input, err := a.LocalShardMapping.CreateIterator(ctx, m, opt)
	if err != nil {
		return nil, err
	}
	inputs = append(inputs, input)

	for _, ic := range a.RemoteShardMap[source] {
		input, err := ic.CreateIterator(ctx, m, opt)
		if err != nil {
			query.Iterators(inputs).Close()
			return nil, err
		}
		inputs = append(inputs, input)
	}

	return query.Iterators(inputs).Merge(opt)
}

func (a *ClusterShardMapping) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	var costs query.IteratorCost

	cost, err := a.LocalShardMapping.IteratorCost(m, opt)
	if err != nil {
		return query.IteratorCost{}, err
	}
	costs = costs.Combine(cost)

	for _, ic := range a.RemoteShardMap[source] {
		cost, err := ic.IteratorCost(m, opt)
		if err != nil {
			return query.IteratorCost{}, err
		}
		costs = costs.Combine(cost)
	}

	return costs, nil
}

// Close clears out the list of mapped shards.
func (a *ClusterShardMapping) Close() error {
	a.LocalShardMapping.Close()
	for _, ics := range a.RemoteShardMap {
		for _, ic := range ics {
			ic.Close()
		}
	}
	a.RemoteShardMap = nil
	return nil
}

// remoteIteratorCreator creates iterators for remote shards.
type remoteIteratorCreator struct {
	dialer   *NodeDialer
	nodeID   uint64
	shardIDs []uint64
	minTime  time.Time
	maxTime  time.Time
}

// newRemoteIteratorCreator returns a new instance of remoteIteratorCreator for a remote shard.
func newRemoteIteratorCreator(dialer *NodeDialer, nodeID uint64, shardIDs []uint64, minTime, maxTime time.Time) *remoteIteratorCreator {
	return &remoteIteratorCreator{
		dialer:   dialer,
		nodeID:   nodeID,
		shardIDs: shardIDs,
		minTime:  minTime,
		maxTime:  maxTime,
	}
}

func (ic *remoteIteratorCreator) FieldDimensions(m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	conn, err := ic.dialer.DialNode(ic.nodeID)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	// Write request.
	if err := rpc.EncodeTLV(conn, rpc.FieldDimensionsRequestMessage, &rpc.FieldDimensionsRequest{
		ShardIDs:    ic.shardIDs,
		Measurement: *m,
	}); err != nil {
		return nil, nil, err
	}

	// Read the response.
	var resp rpc.FieldDimensionsResponse
	if _, err := rpc.DecodeTLV(conn, &resp); err != nil {
		return nil, nil, err
	}
	return resp.Fields, resp.Dimensions, resp.Err
}

func (ic *remoteIteratorCreator) MapType(m *influxql.Measurement, field string) influxql.DataType {
	conn, err := ic.dialer.DialNode(ic.nodeID)
	if err != nil {
		return influxql.Unknown
	}
	defer conn.Close()

	// Write request.
	if err := rpc.EncodeTLV(conn, rpc.MapTypeRequestMessage, &rpc.MapTypeRequest{
		ShardIDs:    ic.shardIDs,
		Measurement: *m,
		Field:       field,
	}); err != nil {
		return influxql.Unknown
	}

	// Read the response.
	var resp rpc.MapTypeResponse
	if _, err := rpc.DecodeTLV(conn, &resp); err != nil {
		return influxql.Unknown
	}
	return resp.Type
}

func (ic *remoteIteratorCreator) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	conn, err := ic.dialer.DialNode(ic.nodeID)
	if err != nil {
		return nil, err
	}

	// Override the time constraints if they don't match each other.
	if !ic.minTime.IsZero() && opt.StartTime < ic.minTime.UnixNano() {
		opt.StartTime = ic.minTime.UnixNano()
	}
	if !ic.maxTime.IsZero() && opt.EndTime > ic.maxTime.UnixNano() {
		opt.EndTime = ic.maxTime.UnixNano()
	}

	var resp rpc.CreateIteratorResponse
	if err := func() error {
		// Write request.
		if err := rpc.EncodeTLV(conn, rpc.CreateIteratorRequestMessage, &rpc.CreateIteratorRequest{
			ShardIDs:    ic.shardIDs,
			Measurement: *m,
			Opt:         opt,
		}); err != nil {
			return err
		}

		// Read the response.
		if _, err := rpc.DecodeTLV(conn, &resp); err != nil {
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

func (ic *remoteIteratorCreator) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	conn, err := ic.dialer.DialNode(ic.nodeID)
	if err != nil {
		return query.IteratorCost{}, err
	}
	defer conn.Close()

	// Override the time constraints if they don't match each other.
	if !ic.minTime.IsZero() && opt.StartTime < ic.minTime.UnixNano() {
		opt.StartTime = ic.minTime.UnixNano()
	}
	if !ic.maxTime.IsZero() && opt.EndTime > ic.maxTime.UnixNano() {
		opt.EndTime = ic.maxTime.UnixNano()
	}

	// Write request.
	if err := rpc.EncodeTLV(conn, rpc.IteratorCostRequestMessage, &rpc.IteratorCostRequest{
		ShardIDs:    ic.shardIDs,
		Measurement: *m,
		Opt:         opt,
	}); err != nil {
		return query.IteratorCost{}, err
	}

	// Read the response.
	var resp rpc.IteratorCostResponse
	if _, err := rpc.DecodeTLV(conn, &resp); err != nil {
		return query.IteratorCost{}, err
	}
	return resp.Cost, resp.Err
}

func (ic *remoteIteratorCreator) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	conn, err := ic.dialer.DialNode(ic.nodeID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Write request.
	if err := rpc.EncodeTLV(conn, rpc.ExpandSourcesRequestMessage, &rpc.ExpandSourcesRequest{
		ShardIDs: ic.shardIDs,
		Sources:  sources,
	}); err != nil {
		return nil, err
	}

	// Read the response.
	var resp rpc.ExpandSourcesResponse
	if _, err := rpc.DecodeTLV(conn, &resp); err != nil {
		return nil, err
	}
	return resp.Sources, resp.Err
}

func (ic *remoteIteratorCreator) Close() error {
	ic.shardIDs = nil
	return nil
}

// NodeDialer dials connections to a given node.
type NodeDialer struct {
	MetaClient MetaClient
	Timeout    time.Duration
}

// DialNode returns a connection to a node.
func (d *NodeDialer) DialNode(nodeID uint64) (net.Conn, error) {
	ni, err := d.MetaClient.DataNode(nodeID)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", ni.TCPAddr)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(d.Timeout))

	// Write the cluster multiplexing header byte
	if _, err := conn.Write([]byte{MuxHeader}); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
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
