package reader

import (
	"gitee.com/menciis/gkit/sys/xxhash3"
)

type Decimal1 = int64

type WeaterStationData struct {
	Name          string
	Min, Max, Sum Decimal1
	Count         int
}

type IdentityHash struct {
	Hash [2]uint64
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

type ProcessedResults = map[IdentityHash]*WeaterStationData

func IterInto(data []byte, results ProcessedResults) {
	var recordMeasurement Decimal1
	pos := 0
	idKey := IdentityHash{}
	for pos < len(data) {
		// Read name
		recordStart := pos
		pos++
		for ; data[pos] != ';'; pos++ {
		}
		idKey.Hash = xxhash3.Hash128(data[recordStart:pos])
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
		if item, ok := results[idKey]; ok {
			item.Count += 1
			item.Sum += recordMeasurement
			item.Min = min(item.Min, recordMeasurement)
			item.Max = max(item.Max, recordMeasurement)
		} else {
			results[idKey] = &WeaterStationData{
				Name:  string(data[recordStart:pos]),
				Min:   recordMeasurement,
				Max:   recordMeasurement,
				Sum:   recordMeasurement,
				Count: 1,
			}
		}
	}
}

func Decimal1ToFloat64(dec Decimal1) float64 {
	return float64(dec) / 10
}
