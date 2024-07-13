package coordinator_test

import (
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

func TestStoreStream_ResultSet(t *testing.T) {
	expectedTimestamps := []int64{101, 102}
	expectedValues := []int64{201, 202}
	pr, pw := io.Pipe()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		stream := coordinator.NewStoreStreamSender(pw)
		defer stream.Close()

		rs := mock.NewResultSet()
		rs.StatsFunc = func() cursors.CursorStats {
			return cursors.CursorStats{
				ScannedValues: 37,
				ScannedBytes:  41,
			}
		}
		nextHasBeenCalledOnce := false
		rs.NextFunc = func() bool { // Returns true exactly once
			if !nextHasBeenCalledOnce {
				nextHasBeenCalledOnce = true
				return true
			}
			return false
		}
		cursorHasBeenCalledOnce := false
		rs.CursorFunc = func() cursors.Cursor {
			if !cursorHasBeenCalledOnce {
				cursorHasBeenCalledOnce = true
				ic := mock.NewIntegerArrayCursor()
				nextHasBeenCalledOnce := false
				ic.NextFunc = func() *cursors.IntegerArray {
					if !nextHasBeenCalledOnce {
						nextHasBeenCalledOnce = true
						return &cursors.IntegerArray{
							Timestamps: expectedTimestamps,
							Values:     expectedValues,
						}
					}
					return &cursors.IntegerArray{}
				}
				return ic
			}
			return nil
		}

		// This is what we're testing.
		rw := reads.NewResponseWriter(stream, 0)
		err := rw.WriteResultSet(rs)
		if err != nil {
			t.Log(err)
			t.Fail()
			return
		}
		rw.Flush()
	}()

	go func() {
		defer wg.Done()
		stream := coordinator.NewStoreStreamReceiver(pr)
		defer stream.Close()

		rr, err := stream.Recv()
		if err != nil {
			t.Log(err)
			t.Fail()
			return
		}

		if len(rr.Frames) != 2 {
			t.Errorf("expected frames length '%v' but got '%v'", 2, len(rr.Frames))
		}
		fs, ok := rr.Frames[0].Data.(*datatypes.ReadResponse_Frame_Series)
		if !ok {
			t.Errorf("expected frame series but got '%v'", rr.Frames[0].Data)
		}
		if fs.Series.DataType != datatypes.DataTypeInteger {
			t.Errorf("expected data type integer but got '%v'", fs.Series.DataType)
		}

		fi, ok := rr.Frames[1].Data.(*datatypes.ReadResponse_Frame_IntegerPoints)
		if !ok {
			t.Errorf("expected frame integer points but got '%v'", rr.Frames[1].Data)
		}
		if !reflect.DeepEqual(fi.IntegerPoints.Timestamps, expectedTimestamps) {
			t.Errorf("expected timestamps '%v' but got '%v'", expectedTimestamps, fi.IntegerPoints.Timestamps)
		}
		if !reflect.DeepEqual(fi.IntegerPoints.Values, expectedValues) {
			t.Errorf("expected values '%v' but got '%v'", expectedValues, fi.IntegerPoints.Values)
		}
	}()

	wg.Wait()
}

func TestStoreStream_GroupResultSet(t *testing.T) {
	expectedTimestamps := []int64{101, 102}
	expectedValues := []int64{201, 202}
	pr, pw := io.Pipe()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		stream := coordinator.NewStoreStreamSender(pw)
		defer stream.Close()

		gc := mock.NewGroupCursor()
		gc.StatsFunc = func() cursors.CursorStats {
			return cursors.CursorStats{
				ScannedValues: 37,
				ScannedBytes:  41,
			}
		}
		cNextHasBeenCalledOnce := false
		gc.NextFunc = func() bool {
			if !cNextHasBeenCalledOnce {
				cNextHasBeenCalledOnce = true
				return true
			}
			return false
		}
		gc.CursorFunc = func() cursors.Cursor {
			ic := mock.NewIntegerArrayCursor()
			nextHasBeenCalledOnce := false
			ic.NextFunc = func() *cursors.IntegerArray {
				if !nextHasBeenCalledOnce {
					nextHasBeenCalledOnce = true
					return &cursors.IntegerArray{
						Timestamps: expectedTimestamps,
						Values:     expectedValues,
					}
				}
				return &cursors.IntegerArray{}
			}
			return ic
		}

		rs := mock.NewGroupResultSet()
		rsNextHasBeenCalledOnce := false
		rs.NextFunc = func() reads.GroupCursor {
			if !rsNextHasBeenCalledOnce {
				rsNextHasBeenCalledOnce = true
				return gc
			}
			return nil
		}

		// This is what we're testing.
		rw := reads.NewResponseWriter(stream, 0)
		err := rw.WriteGroupResultSet(rs)
		if err != nil {
			t.Log(err)
			t.Fail()
			return
		}
		rw.Flush()
	}()

	go func() {
		defer wg.Done()
		stream := coordinator.NewStoreStreamReceiver(pr)
		defer stream.Close()

		rr, err := stream.Recv()
		if err != nil {
			t.Log(err)
			t.Fail()
			return
		}

		if len(rr.Frames) != 3 {
			t.Errorf("expected frames length '%v' but got '%v'", 2, len(rr.Frames))
		}
		_, ok := rr.Frames[0].Data.(*datatypes.ReadResponse_Frame_Group)
		if !ok {
			t.Errorf("expected frame series but got '%v'", rr.Frames[0].Data)
		}

		fs, ok := rr.Frames[1].Data.(*datatypes.ReadResponse_Frame_Series)
		if !ok {
			t.Errorf("expected frame series but got '%v'", rr.Frames[1].Data)
		}
		if fs.Series.DataType != datatypes.DataTypeInteger {
			t.Errorf("expected data type integer but got '%v'", fs.Series.DataType)
		}

		fi, ok := rr.Frames[2].Data.(*datatypes.ReadResponse_Frame_IntegerPoints)
		if !ok {
			t.Errorf("expected frame integer points but got '%v'", rr.Frames[2].Data)
		}
		if !reflect.DeepEqual(fi.IntegerPoints.Timestamps, expectedTimestamps) {
			t.Errorf("expected timestamps '%v' but got '%v'", expectedTimestamps, fi.IntegerPoints.Timestamps)
		}
		if !reflect.DeepEqual(fi.IntegerPoints.Values, expectedValues) {
			t.Errorf("expected values '%v' but got '%v'", expectedValues, fi.IntegerPoints.Values)
		}
	}()

	wg.Wait()
}
