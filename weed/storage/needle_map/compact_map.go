package needle_map

import (
	"cmp"
	"fmt"
	"slices"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type CompactOffset [types.OffsetSize]byte
type CompactNeedleValue struct {
	offset CompactOffset
	size   types.Size
}

type CompactMap struct {
	sync.RWMutex

	values map[types.NeedleId]CompactNeedleValue
}

func OffsetToCompact(offset types.Offset) CompactOffset {
	var co CompactOffset
	types.OffsetToBytes(co[:], offset)
	return co
}

func (co CompactOffset) Offset() types.Offset {
	return types.BytesToOffset(co[:])
}

func NewCompactMap() *CompactMap {
	return &CompactMap{
		values: map[types.NeedleId]CompactNeedleValue{},
	}
}

func (cm *CompactMap) Len() int {
	return len(cm.values)
}

func (cm *CompactMap) String() string {
	return fmt.Sprintf("%d elements", cm.Len())
}

func (cm *CompactMap) needleValue(key types.NeedleId) NeedleValue {
	cnv := cm.values[key]
	return NeedleValue{
		Key:    key,
		Offset: cnv.offset.Offset(),
		Size:   cnv.size,
	}
}

// Set inserts/updates a NeedleValue.
// If the operation is an update, returns the overwritten value's previous offset and size.
func (cm *CompactMap) Set(key types.NeedleId, offset types.Offset, size types.Size) (oldOffset types.Offset, oldSize types.Size) {
	cm.RLock()
	defer cm.RUnlock()

	if nv, ok := cm.values[key]; ok {
		o := nv.offset.Offset()
		oldOffset.OffsetLower = o.OffsetLower
		oldOffset.OffsetHigher = o.OffsetHigher
		oldSize = nv.size
	}

	cm.values[key] = CompactNeedleValue{
		offset: OffsetToCompact(offset),
		size:   size,
	}

	return
}

// Get seeks a map entry by key. Returns an entry pointer, with a boolean specifiying if the entry was found.
func (cm *CompactMap) Get(key types.NeedleId) (*NeedleValue, bool) {
	cm.RLock()
	defer cm.RUnlock()

	if _, ok := cm.values[key]; ok {
		nv := cm.needleValue(key)
		return &nv, true
	}

	return nil, false
}

// Delete deletes a map entry by key. Returns the entries' previous Size, if available.
func (cm *CompactMap) Delete(key types.NeedleId) types.Size {
	cm.RLock()
	defer cm.RUnlock()

	if cnv, ok := cm.values[key]; ok {
		if cnv.size > 0 && cnv.size.IsValid() {
			cm.values[key] = CompactNeedleValue{
				offset: cnv.offset,
				size:   -cnv.size,
			}
			return cnv.size
		}
	}

	return types.Size(0)
}

// AscendingVisit runs a function on all entries, in ascending key order. Returns any errors hit while visiting.
func (cm *CompactMap) AscendingVisit(visit func(NeedleValue) error) error {
	cm.RLock()
	defer cm.RUnlock()

	keys := []types.NeedleId{}
	for key := range cm.values {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b types.NeedleId) int {
		return cmp.Compare(uint64(a), uint64(b))
	})

	for _, k := range keys {
		nv := cm.needleValue(k)
		if err := visit(nv); err != nil {
			return err
		}
	}
	return nil
}
