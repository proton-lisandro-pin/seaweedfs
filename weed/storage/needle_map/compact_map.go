package needle_map

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type CompactMap struct {
	sync.RWMutex

	list     []NeedleValue
	firstKey types.NeedleId
	lastKey  types.NeedleId
}

func NewCompactMap() *CompactMap {
	return &CompactMap{
		list:     []NeedleValue{},
		firstKey: types.NeedleIdMax,
		lastKey:  types.NeedleIdMin,
	}
}

func (cm *CompactMap) Len() int {
	return len(cm.list)
}

func (cm *CompactMap) Cap() int {
	return cap(cm.list)
}

// bsearchKey returns the NeedleValue index for a given key.
// If the key is not found, it returns the index where it should be inserted instead.
func (cm *CompactMap) bsearchKey(key types.NeedleId) (int, bool) {
	switch {
	case len(cm.list) == 0:
		return 0, false
	case key == cm.firstKey:
		return 0, true
	case key <= cm.firstKey:
		return 0, false
	case key == cm.lastKey:
		return len(cm.list) - 1, true
	case key > cm.lastKey:
		return len(cm.list), false
	}

	low := 0
	high := len(cm.list) - 1
	mid := 0

	for low <= high {
		mid = (high + low) / 2
		lkey := cm.list[mid].Key
		switch {
		case lkey == key:
			return mid, true
		case lkey < key:
			low = mid + 1
			continue
		case lkey > key:
			high = mid - 1
			continue
		}
	}

	// account for integer division rounding when computing mid - this doesn't affect
	// the binary search, but can shift the insert point for new values.
	if mid < len(cm.list)-1 && key >= cm.list[mid].Key {
		mid += 1
	}

	return mid, false
}

// Set inserts/updates a NeedleValue.
// If the operation is an update, returns the overwritten value's previous offset and size.
func (cm *CompactMap) Set(key types.NeedleId, offset types.Offset, size types.Size) (oldOffset types.Offset, oldSize types.Size) {
	cm.RLock()
	defer cm.RUnlock()

	i, found := cm.bsearchKey(key)
	if found {
		// update
		oldOffset.OffsetLower = cm.list[i].Offset.OffsetLower
		oldOffset.OffsetHigher = cm.list[i].Offset.OffsetHigher
		oldSize = cm.list[i].Size

		cm.list[i].Size = size
		cm.list[i].Offset.OffsetLower = offset.OffsetLower
		cm.list[i].Offset.OffsetHigher = offset.OffsetHigher
		return
	}

	if i == len(cm.list) {
		// insert last
		cm.list = append(cm.list, NeedleValue{
			Key: key,
			Offset: types.Offset{
				OffsetLower:  offset.OffsetLower,
				OffsetHigher: offset.OffsetHigher,
			},
			Size: size,
		})
	} else {
		// insert in middle
		cm.list = append(cm.list, NeedleValue{})
		copy(cm.list[i+1:], cm.list[i:])
		cm.list[i].Key = key
		cm.list[i].Offset.OffsetLower = offset.OffsetLower
		cm.list[i].Offset.OffsetHigher = offset.OffsetHigher
		cm.list[i].Size = size
	}

	if key < cm.firstKey {
		cm.firstKey = key
	}
	if key > cm.lastKey {
		cm.lastKey = key
	}

	return
}

// Get seeks a map entry by key. Returns an entry pointer, with a boolean specifiying if the entry was found.
func (cm *CompactMap) Get(key types.NeedleId) (*NeedleValue, bool) {
	cm.RLock()
	defer cm.RUnlock()

	if i, found := cm.bsearchKey(key); found {
		return &cm.list[i], true
	}

	return nil, false
}

// Delete deletes a map entry by key. Returns the entries' previous Size, if available.
func (cm *CompactMap) Delete(key types.NeedleId) types.Size {
	cm.RLock()
	defer cm.RUnlock()

	if i, found := cm.bsearchKey(key); found {
		if cm.list[i].Size > 0 && cm.list[i].Size.IsValid() {
			ret := cm.list[i].Size
			cm.list[i].Size = -cm.list[i].Size
			return ret
		}
	}

	return types.Size(0)
}

// AscendingVisit runs a function on all entries, in order. Returns any errors while visiting.
func (cm *CompactMap) AscendingVisit(visit func(NeedleValue) error) error {
	cm.RLock()
	defer cm.RUnlock()

	for _, nv := range cm.list {
		if err := visit(nv); err != nil {
			return err
		}
	}
	return nil
}
