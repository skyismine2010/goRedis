package goRedis

import (
	"fmt"
	"math"
)

const DictHTInitialSize = 4
const DictForceResizeRatio = 5

type dictEntry struct {
	key  interface{}
	val  interface{}
	next *dictEntry
}

type dictHT struct {
	table    []*dictEntry
	sizeMask uint64
	used     uint64
	size     uint64
}

type dict struct {
	dType     *DictType
	ht        [2]dictHT
	rehashIdx int64
	iterators uint64
}

type hashFunctionType func(key interface{}) (uint64, error)

type keyDupType func(key interface{}) (interface{}, error)
type valDupType func(val interface{}) (interface{}, error)
type keyCompareType func(k1 interface{}, k2 interface{}) (bool, error)

type DictType struct {
	hashFunction hashFunctionType
	keyDup       keyDupType
	valDup       keyDupType
	keyCompare   keyCompareType
}

func dictHTReset(ht *dictHT) {
	ht.table = nil
	ht.sizeMask = 0
	ht.used = 0
}

func dictIsRehashing(d *dict) bool {
	return d.rehashIdx != -1
}

func dictCreate(t *DictType) *dict {
	var d dict
	d.dType = t
	dictHTReset(&d.ht[0])
	dictHTReset(&d.ht[1])
	d.rehashIdx = -1
	d.iterators = 0
	return &d
}

func dictFind(d *dict, key interface{}) interface{} {
	if d.ht[0].used == 0 && d.ht[1].used == 0 {
		return nil
	}

	hashKey, err := d.dType.hashFunction(key)
	if err != nil {
		panic(err)
	}
	for i := 0; i <= 1; i++ {
		idx := hashKey & d.ht[i].sizeMask
		entry := d.ht[i].table[idx]
		for entry != nil {
			isSame, _ := d.dType.keyCompare(key, entry.key)
			if (key == entry.key) || isSame {
				return &entry.val
			}
			entry = entry.next
		}
	}
	return nil
}

func dictNextPower(size uint64) uint64 {
	var i uint64
	i = DictHTInitialSize

	if size == math.MaxInt64 {
		//todo: how to handle this sitution?
		return size
	}
	for {
		if i >= size {
			return i
		}
		i *= 2
	}
}

func dictExpand(d *dict, size uint64) error {
	if dictIsRehashing(d) || d.ht[0].used > size {
		return fmt.Errorf("no need do hash table expand operation. d = %v", d)
	}
	realSize := dictNextPower(size)
	if realSize == d.ht[0].size {
		return fmt.Errorf("can't expand hash table, realsize=%d", realSize)
	}
	var ht dictHT
	ht.table = make([]*dictEntry, realSize)
	ht.size = realSize
	ht.sizeMask = realSize - 1
	ht.used = 0

	if d.ht[0].table == nil {
		d.ht[0] = ht
		return nil
	}
	d.ht[1] = ht
	d.rehashIdx = 0
	return nil
}

func dictExpandIfNeed(d *dict) error {
	if dictIsRehashing(d) {
		return nil
	}

	if d.ht[0].size == 0 {
		return dictExpand(d, DictHTInitialSize)
	}

	//todo:先暂时不管
	if d.ht[0].used >= d.ht[0].size {
		return dictExpand(d, d.ht[0].used*2)
	}

	return nil

}

func dictAdd(d *dict, key interface{}, val interface{}) {
	panic("Implement me.")
}

func dictAddEntry(d *dict, key interface{}) *dictEntry {

	panic("Implement me.")
}
