package main

import (
	"iter"

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

var DECIMAL_1 = [58]int64{
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
}
var DECIMAL_10 = [58]int64{
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 10, 20, 30, 40, 50, 60, 70, 80, 90,
}
var DECIMAL_100 = [58]int64{
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 100, 200, 300, 400, 500, 600, 700, 800, 900,
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

func IterInto(data []byte, results *ProcessedResults) {
	var recordMeasurement Decimal1
	pos := 0
	for pos < len(data) {
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
		// Numbers are either length 3 or 4, ie. x.x or xx.x
		length3 := data[pos+3] == '\n'
		if length3 {
			recordMeasurement = DECIMAL_10[data[pos]] + DECIMAL_1[data[pos+2]] | negativizer
			pos += 4
		} else {
			// If not competition code, should check and error on (data[pos+4] != '\n')
			recordMeasurement = DECIMAL_100[data[pos]] + DECIMAL_10[data[pos+1]] + DECIMAL_1[data[pos+3]] | negativizer
			pos += 5
		}
		// Update map
		item.Count += 1
		item.Sum += recordMeasurement
		item.Min = min(item.Min, recordMeasurement)
		item.Max = max(item.Max, recordMeasurement)
	}
}

func Decimal1ToFloat64(dec Decimal1) float64 {
	return float64(dec) / 10
}
