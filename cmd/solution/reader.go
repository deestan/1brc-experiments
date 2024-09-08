package main

import (
	"fmt"
	"iter"
	"unsafe"

	"gitee.com/menciis/gkit/sys/xxhash3"
)

type Decimal1 = int64

type IdentityHash = [2]uint64

type WeatherStationData struct {
	Id            IdentityHash
	Name          string
	Min, Max, Sum Decimal1
	Count         int
}

type ProcessedResults struct {
	lists   [65536 + 10000]CollisionList
	freePos int
}

type CollisionList struct {
	v    WeatherStationData
	next *CollisionList
}

func NewProcessedResults() ProcessedResults {
	return ProcessedResults{freePos: 65536}
}

func (p *ProcessedResults) get(name []byte) *WeatherStationData {
	id := xxhash3.Hash128(name)
	lookupHash := uint16(id[0])
	collisionList := &p.lists[lookupHash]
	if collisionList.v.Count == 0 {
		item := &collisionList.v
		item.Name = string(name)
		item.Id = id
		item.Min = 999
		item.Max = -999
		return item
	}
	for {
		if collisionList.v.Id[0] == id[0] && collisionList.v.Id[1] == id[1] {
			return &collisionList.v
		}
		if collisionList.next == nil {
			break
		}
		collisionList = collisionList.next
	}
	collisionList.next = &p.lists[p.freePos]
	p.freePos++
	item := &collisionList.next.v
	item.Name = string(name)
	item.Id = id
	item.Min = 999
	item.Max = -999
	return item
}

func (p *ProcessedResults) Entries() iter.Seq[*WeatherStationData] {
	return func(yield func(*WeatherStationData) bool) {
		for i := range p.lists {
			list := &p.lists[i]
			for list != nil {
				if list.v.Count != 0 {
					if !yield(&list.v) {
						return
					}
				}
				list = list.next
			}
		}
	}
}

func IterInto(data []byte, results *ProcessedResults, numberLookup *[65536]Decimal1) {
	pos := int64(0)
	end := int64(len(data))
	for pos < end {
		// Read name
		recordStart := pos
		pos++
		for ; data[pos] != ';'; pos++ {
		}
		item := results.get(data[recordStart:pos])
		pos++
		// Read measurement
		negativizer := int64(0)
		if data[pos] == '-' {
			pos += 1
			negativizer = -1
		}
		foldedLookup := fold((*uint32)(unsafe.Pointer(&data[pos])))
		num := numberLookup[foldedLookup]
		recordMeasurement := num&0x3ff | negativizer
		pos += num >> 10
		// Update map
		item.Count += 1
		item.Sum += recordMeasurement
		item.Min = min(item.Min, recordMeasurement)
		item.Max = max(item.Max, recordMeasurement)
	}
}

func PrepareLookup() [65536]Decimal1 {
	v := [65536]Decimal1{}
	for i := range 1000 {
		s := fmt.Sprintf("%d.%d\n", i/10, i%10)
		b := []byte(s)
		foldedLookup := fold((*uint32)(unsafe.Pointer(&b[0])))
		skip := int64(len(s) << 10)
		v[foldedLookup] = Decimal1(i) | skip
	}
	return v
}

func fold(val *uint32) uint16 {
	v := *val & 0x0f0f0f0f
	return uint16(v) | uint16(v>>12)
}

func Decimal1ToFloat64(dec Decimal1) float64 {
	return float64(dec) / 10
}
