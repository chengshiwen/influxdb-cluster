// Code generated by "stringer -type=MessageType"; DO NOT EDIT.

package binary

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[HeaderType-1]
	_ = x[BucketHeaderType-2]
	_ = x[BucketFooterType-3]
	_ = x[SeriesHeaderType-4]
	_ = x[FloatPointsType-5]
	_ = x[IntegerPointsType-6]
	_ = x[UnsignedPointsType-7]
	_ = x[BooleanPointsType-8]
	_ = x[StringPointsType-9]
	_ = x[SeriesFooterType-10]
}

const _MessageType_name = "HeaderTypeBucketHeaderTypeBucketFooterTypeSeriesHeaderTypeFloatPointsTypeIntegerPointsTypeUnsignedPointsTypeBooleanPointsTypeStringPointsTypeSeriesFooterType"

var _MessageType_index = [...]uint8{0, 10, 26, 42, 58, 73, 90, 108, 125, 141, 157}

func (i MessageType) String() string {
	i -= 1
	if i >= MessageType(len(_MessageType_index)-1) {
		return "MessageType(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _MessageType_name[_MessageType_index[i]:_MessageType_index[i+1]]
}
