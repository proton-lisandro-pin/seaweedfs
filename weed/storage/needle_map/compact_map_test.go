package needle_map

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestSegmentSet(t *testing.T) {
	testMap := &CompactMap{
		values: map[types.NeedleId]CompactNeedleValue{
			10: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			20: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			30: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
		},
	}

	if got, want := testMap.Len(), 3; got != want {
		t.Errorf("got starting size %d, want %d", got, want)
	}

	testSets := []struct {
		name       string
		key        types.NeedleId
		offset     types.Offset
		size       types.Size
		wantOffset types.Offset
		wantSize   types.Size
	}{
		{
			name: "insert at beggining",
			key:  5, offset: types.Uint32ToOffset(1000), size: 123,
			wantOffset: types.Uint32ToOffset(0), wantSize: 0,
		},
		{
			name: "insert at end",
			key:  51, offset: types.Uint32ToOffset(7000), size: 456,
			wantOffset: types.Uint32ToOffset(0), wantSize: 0,
		},
		{
			name: "insert in middle",
			key:  25, offset: types.Uint32ToOffset(8000), size: 789,
			wantOffset: types.Uint32ToOffset(0), wantSize: 0,
		},
		{
			name: "update existing",
			key:  30, offset: types.Uint32ToOffset(9000), size: 999,
			wantOffset: types.Uint32ToOffset(300), wantSize: 300,
		},
	}

	for _, ts := range testSets {
		offset, size := testMap.Set(ts.key, ts.offset, ts.size)
		if offset != ts.wantOffset {
			t.Errorf("%s: got offset %v, want %v", ts.name, offset, ts.wantOffset)
		}
		if size != ts.wantSize {
			t.Errorf("%s: got size %v, want %v", ts.name, size, ts.wantSize)
		}
	}

	wantMap := &CompactMap{
		values: map[types.NeedleId]CompactNeedleValue{
			5:  CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(1000)), size: 123},
			10: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			20: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			25: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(8000)), size: 789},
			30: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(9000)), size: 999},
			51: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(7000)), size: 456},
		},
	}
	if !reflect.DeepEqual(testMap, wantMap) {
		t.Errorf("got result segment %v, want %v", testMap, wantMap)
	}

	if got, want := testMap.Len(), 6; got != want {
		t.Errorf("got result size %d, want %d", got, want)
	}
}

func TestSegmentGet(t *testing.T) {
	testMap := &CompactMap{
		values: map[types.NeedleId]CompactNeedleValue{
			10: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			20: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			30: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
		},
	}

	testCases := []struct {
		name      string
		key       types.NeedleId
		wantValue *NeedleValue
		wantFound bool
	}{
		{
			name:      "invalid key",
			key:       99,
			wantValue: nil,
			wantFound: false,
		},
		{
			name:      "key #1",
			key:       10,
			wantValue: &NeedleValue{Key: 10, Offset: types.Uint32ToOffset(0), Size: 100},
			wantFound: true,
		},
		{
			name:      "key #2",
			key:       20,
			wantValue: &NeedleValue{Key: 20, Offset: types.Uint32ToOffset(100), Size: 200},
			wantFound: true,
		},
		{
			name:      "key #3",
			key:       30,
			wantValue: &NeedleValue{Key: 30, Offset: types.Uint32ToOffset(300), Size: 300},
			wantFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value, found := testMap.Get(tc.key)
			if got, want := found, tc.wantFound; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if tc.wantValue != nil {
				if got, want := *value, *tc.wantValue; got != want {
					t.Errorf("got %v, want %v", got, want)
				}
			} else {
				if value != nil {
					t.Errorf("got %v, want nil", value)
				}
			}
		})
	}
}

func TestSegmentDelete(t *testing.T) {
	testMap := &CompactMap{
		values: map[types.NeedleId]CompactNeedleValue{
			10: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			20: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			30: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
			40: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(600)), size: 400},
		},
	}

	testDeletes := []struct {
		name string
		key  types.NeedleId
		want types.Size
	}{
		{
			name: "invalid key",
			key:  99,
			want: 0,
		},
		{
			name: "delete key #2",
			key:  20,
			want: 200,
		},
		{
			name: "delete key #4",
			key:  40,
			want: 400,
		},
	}

	for _, td := range testDeletes {
		size := testMap.Delete(td.key)
		if got, want := size, td.want; got != want {
			t.Errorf("%s: got %v, want %v", td.name, got, want)
		}
	}

	wantMap := &CompactMap{
		values: map[types.NeedleId]CompactNeedleValue{
			10: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			20: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(100)), size: -200},
			30: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
			40: CompactNeedleValue{offset: OffsetToCompact(types.Uint32ToOffset(600)), size: -400},
		},
	}
	if !reflect.DeepEqual(testMap, wantMap) {
		t.Errorf("got result segment %v, want %v", testMap, wantMap)
	}
}

func TestAscendingVisit(t *testing.T) {
	cm := NewCompactMap()
	for _, nid := range []types.NeedleId{20, 7, 40000, 300000, 0, 100, 500, 10000, 200000} {
		cm.Set(nid, types.Uint32ToOffset(123), 456)
	}

	got := []NeedleValue{}
	err := cm.AscendingVisit(func(nv NeedleValue) error {
		got = append(got, nv)
		return nil
	})
	if err != nil {
		t.Errorf("got error %v, expected none", err)
	}

	want := []NeedleValue{
		NeedleValue{Key: 0, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 7, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 20, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 100, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 500, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 10000, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 40000, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 200000, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 300000, Offset: types.Uint32ToOffset(123), Size: 456},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got values %v, want %v", got, want)
	}
}

func TestOrdering(t *testing.T) {
	count := 400000
	keys := []types.NeedleId{}
	for i := 0; i < count; i++ {
		keys = append(keys, types.NeedleId(i))
	}

	r := rand.New(rand.NewSource(123456789))
	r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	cm := NewCompactMap()
	for _, k := range keys {
		_, _ = cm.Set(k, types.Uint32ToOffset(123), 456)
	}
	if got, want := cm.Len(), count; got != want {
		t.Errorf("expected size %d, got %d", want, got)
	}

	last := -1
	err := cm.AscendingVisit(func(nv NeedleValue) error {
		key := int(nv.Key)
		if key <= last {
			return fmt.Errorf("found out of order entries (%d vs %d)", key, last)
		}
		last = key
		return nil
	})
	if err != nil {
		t.Errorf("got error %v, expected none", err)
	}
}
