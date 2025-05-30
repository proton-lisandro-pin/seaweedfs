package needle_map

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestBsearchKey(t *testing.T) {
	testMap := &CompactMap{
		list: []NeedleValue{
			NeedleValue{Key: 10},
			NeedleValue{Key: 20},
			NeedleValue{Key: 21},
			NeedleValue{Key: 26},
			NeedleValue{Key: 30},
		},
		firstKey: 10,
		lastKey:  30,
	}

	testCases := []struct {
		name      string
		cm        *CompactMap
		key       types.NeedleId
		wantIndex int
		wantFound bool
	}{
		{
			name:      "empty map",
			cm:        NewCompactMap(),
			key:       123,
			wantIndex: 0,
			wantFound: false,
		},
		{
			name:      "new key, insert at beggining",
			cm:        testMap,
			key:       5,
			wantIndex: 0,
			wantFound: false,
		},
		{
			name:      "new key, insert at end",
			cm:        testMap,
			key:       100,
			wantIndex: 5,
			wantFound: false,
		},
		{
			name:      "new key, insert second",
			cm:        testMap,
			key:       12,
			wantIndex: 1,
			wantFound: false,
		},
		{
			name:      "new key, insert in middle",
			cm:        testMap,
			key:       23,
			wantIndex: 3,
			wantFound: false,
		},
		{
			name:      "key #1",
			cm:        testMap,
			key:       10,
			wantIndex: 0,
			wantFound: true,
		},
		{
			name:      "key #2",
			cm:        testMap,
			key:       20,
			wantIndex: 1,
			wantFound: true,
		},
		{
			name:      "key #3",
			cm:        testMap,
			key:       21,
			wantIndex: 2,
			wantFound: true,
		},
		{
			name:      "key #4",
			cm:        testMap,
			key:       26,
			wantIndex: 3,
			wantFound: true,
		},
		{
			name:      "key #5",
			cm:        testMap,
			key:       30,
			wantIndex: 4,
			wantFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			index, found := tc.cm.bsearchKey(tc.key)
			if got, want := index, tc.wantIndex; got != want {
				t.Errorf("expected %v, got %v", want, got)
			}
			if got, want := found, tc.wantFound; got != want {
				t.Errorf("expected %v, got %v", want, got)
			}
		})
	}
}

func TestSet(t *testing.T) {
	testMap := &CompactMap{
		list: []NeedleValue{
			NeedleValue{Key: 10, Offset: types.Uint32ToOffset(0), Size: 100},
			NeedleValue{Key: 20, Offset: types.Uint32ToOffset(100), Size: 200},
			NeedleValue{Key: 30, Offset: types.Uint32ToOffset(300), Size: 300},
		},
		firstKey: 10,
		lastKey:  30,
	}

	if got, want := testMap.Len(), 3; got != want {
		t.Errorf("got starting size %d, want %d", got, want)
	}
	if got, want := testMap.Cap(), 3; got != want {
		t.Errorf("got starting capacity %d, want %d", got, want)
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
		list: []NeedleValue{
			NeedleValue{Key: 5, Offset: types.Uint32ToOffset(1000), Size: 123},
			NeedleValue{Key: 10, Offset: types.Uint32ToOffset(0), Size: 100},
			NeedleValue{Key: 20, Offset: types.Uint32ToOffset(100), Size: 200},
			NeedleValue{Key: 25, Offset: types.Uint32ToOffset(8000), Size: 789},
			NeedleValue{Key: 30, Offset: types.Uint32ToOffset(9000), Size: 999},
			NeedleValue{Key: 51, Offset: types.Uint32ToOffset(7000), Size: 456},
		},
		firstKey: 5,
		lastKey:  51,
	}
	if !reflect.DeepEqual(testMap, wantMap) {
		t.Errorf("got result map %v, want %v", testMap, wantMap)
	}

	if got, want := testMap.Len(), 6; got != want {
		t.Errorf("got result size %d, want %d", got, want)
	}
	if got, want := testMap.Cap(), 6; got != want {
		t.Errorf("got result capacity %d, want %d", got, want)
	}
}

func TestSetOrdering(t *testing.T) {
	count := 100000
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
	for i := 1; i < cm.Len(); i++ {
		if ka, kb := cm.list[i-1].Key, cm.list[i].Key; ka >= kb {
			t.Errorf("found out of order entries at (%d, %d) = (%d, %d)", i-1, i, ka, kb)
		}
	}
}

func TestGet(t *testing.T) {
	testMap := &CompactMap{
		list: []NeedleValue{
			NeedleValue{Key: 10, Offset: types.Uint32ToOffset(0), Size: 100},
			NeedleValue{Key: 20, Offset: types.Uint32ToOffset(100), Size: 200},
			NeedleValue{Key: 30, Offset: types.Uint32ToOffset(300), Size: 300},
		},
		firstKey: 10,
		lastKey:  30,
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
			wantValue: &testMap.list[0],
			wantFound: true,
		},
		{
			name:      "key #2",
			key:       20,
			wantValue: &testMap.list[1],
			wantFound: true,
		},
		{
			name:      "key #3",
			key:       30,
			wantValue: &testMap.list[2],
			wantFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value, found := testMap.Get(tc.key)
			if got, want := value, tc.wantValue; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := found, tc.wantFound; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	testMap := &CompactMap{
		list: []NeedleValue{
			NeedleValue{Key: 10, Offset: types.Uint32ToOffset(0), Size: 100},
			NeedleValue{Key: 20, Offset: types.Uint32ToOffset(100), Size: 200},
			NeedleValue{Key: 30, Offset: types.Uint32ToOffset(300), Size: 300},
			NeedleValue{Key: 40, Offset: types.Uint32ToOffset(600), Size: 400},
		},
		firstKey: 10,
		lastKey:  40,
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
		list: []NeedleValue{
			NeedleValue{Key: 10, Offset: types.Uint32ToOffset(0), Size: 100},
			NeedleValue{Key: 20, Offset: types.Uint32ToOffset(100), Size: -200},
			NeedleValue{Key: 30, Offset: types.Uint32ToOffset(300), Size: 300},
			NeedleValue{Key: 40, Offset: types.Uint32ToOffset(600), Size: -400},
		},
		firstKey: 10,
		lastKey:  40,
	}
	if !reflect.DeepEqual(testMap, wantMap) {
		t.Errorf("got result map %v, want %v", testMap, wantMap)
	}
}

func TestAscendingVisit(t *testing.T) {
	testMap := &CompactMap{
		list: []NeedleValue{
			NeedleValue{Key: 10, Offset: types.Uint32ToOffset(0), Size: 100},
			NeedleValue{Key: 20, Offset: types.Uint32ToOffset(100), Size: 200},
			NeedleValue{Key: 30, Offset: types.Uint32ToOffset(300), Size: 300},
			NeedleValue{Key: 40, Offset: types.Uint32ToOffset(600), Size: 300},
		},
		firstKey: 10,
		lastKey:  40,
	}

	seen := []NeedleValue{}
	err := testMap.AscendingVisit(func(nv NeedleValue) error {
		seen = append(seen, nv)
		return nil
	})
	if err != nil {
		t.Errorf("got error %v, expected none", err)
	}

	if got, want := seen, testMap.list; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
