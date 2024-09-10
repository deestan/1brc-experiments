package main

import (
	"fmt"
	"iter"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

type Decimal1_64 = int64
type Decimal1_16 = int16

type IdentityHash uint64

type WeatherStationData struct {
	Id       IdentityHash
	Name     string
	Sum      Decimal1_64
	Count    uint32
	Min, Max Decimal1_16
}

func (w *WeatherStationData) Empty() bool {
	return w.Count == 0
}

func (w *WeatherStationData) Update(measurement Decimal1_16) {
	w.Count += 1
	w.Sum += Decimal1_64(measurement)
	w.Min = min(w.Min, measurement)
	w.Max = max(w.Max, measurement)
}

const MAP_SIZE = 32768

type ProcessedResults struct {
	items [MAP_SIZE]WeatherStationData
}

func (p *ProcessedResults) MergeFrom(q *ProcessedResults) {
	for i := range q.items {
		if q.items[i].Count == 0 {
			continue
		}
		if pItem, newItem := p.get(q.items[i].Id); newItem != nil {
			*newItem = q.items[i]
		} else {
			pItem.Count += q.items[i].Count
			pItem.Sum += q.items[i].Sum
			pItem.Min = min(pItem.Min, q.items[i].Min)
			pItem.Max = min(pItem.Max, q.items[i].Max)
		}
	}
}

func (p *ProcessedResults) get(id IdentityHash) (*WeatherStationData, *WeatherStationData) {
	index := uint16(id) >> 1
	if p.items[index].Count == 0 {
		return nil, &p.items[index]
	}
	for {
		if p.items[index].Id == id {
			return &p.items[index], nil
		}
		index = index + 1
		if p.items[index].Count == 0 {
			break
		}
	}
	return nil, &p.items[index]
}

func (p *ProcessedResults) Entries() iter.Seq[*WeatherStationData] {
	return func(yield func(*WeatherStationData) bool) {
		for i := range p.items {
			if !p.items[i].Empty() {
				if !yield(&p.items[i]) {
					return
				}
			}
		}
	}
}

func noDelimInFirst8(data *byte) bool {
	n := *(*uint64)(unsafe.Pointer(data)) ^ (';' * 0x0101010101010101)
	return (n-0x0101010101010101)&^n&0x8080808080808080 == 0
}

func IterInto(data []byte, results *ProcessedResults, numberLookup *[65536]Decimal1_16) {
	pos := 0
	end := len(data)
	for pos < end {
		// Read name
		recordStart := pos
		pos++
		for pos < end-8 && noDelimInFirst8(&data[pos]) {
			pos += 8
		}
		for ; data[pos] != ';'; pos++ {
		}
		name := data[recordStart:pos]
		id := IdentityHash(xxhash.Sum64(name))
		pos++
		// Read measurement
		negativizer := int16(0)
		if data[pos] == '-' {
			pos += 1
			negativizer = -1
		}
		foldedLookup := fold((*uint32)(unsafe.Pointer(&data[pos])))
		item, newItem := results.get(id)
		num := numberLookup[foldedLookup]
		recordMeasurement := num&0x3ff | negativizer
		pos += int(num >> 10)
		// Update map
		if newItem != nil {
			newItem.Name = string(name)
			newItem.Id = id
			newItem.Min = recordMeasurement
			newItem.Max = recordMeasurement
			newItem.Sum = Decimal1_64(recordMeasurement)
			newItem.Count = 1
		} else {
			item.Update(recordMeasurement)
		}
	}
}

func PrepareDecimal1Lookup() [65536]Decimal1_16 {
	v := [65536]Decimal1_16{}
	for i := range 1000 {
		s := fmt.Sprintf("%d.%d\n", i/10, i%10)
		b := []byte(s)
		foldedLookup := fold((*uint32)(unsafe.Pointer(&b[0])))
		skip := int16(len(s) << 10)
		v[foldedLookup] = Decimal1_16(i) | skip
	}
	return v
}

func fold(val *uint32) uint16 {
	v := *val & 0x0f0f0f0f
	return uint16(v) | uint16(v>>12)
}

func Decimal1_64ToFloat(dec Decimal1_64) float64 {
	return float64(dec) / 10
}

func Decimal1_16ToFloat(dec Decimal1_16) float64 {
	return float64(dec) / 10
}
