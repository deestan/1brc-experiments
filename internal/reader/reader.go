package reader

import (
	"strings"
	"unsafe"
)

type Decimal1 = int64

type WeaterStationData struct {
	Min, Max, Sum Decimal1
	Count         int
}

type MutableString struct {
	Data unsafe.Pointer
	Len  int
}

type ProcessedResults = map[string]*WeaterStationData

func IterInto(data []byte, results ProcessedResults) {
	var recordName string
	var recordMeasurement Decimal1
	mutableName := (*MutableString)(unsafe.Pointer(&recordName))
	pos := 0
	for pos < len(data) {
		// Read name
		recordStart := pos
		pos++
		for ; data[pos] != ';'; pos++ {
		}
		mutableName.Data = unsafe.Pointer(&data[recordStart])
		mutableName.Len = pos - recordStart
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
			recordMeasurement = (10*int64(data[pos]-'0') + int64(data[pos+2]-'0')) | negativizer
			pos += 4
		} else {
			// If not competition code, should check and error on (data[pos+4] != '\n')
			recordMeasurement = (100*int64(data[pos]-'0') + 10*int64(data[pos+1]-'0') + int64(data[pos+3]-'0')) | negativizer
			pos += 5
		}

		if item, ok := results[recordName]; ok {
			item.Count += 1
			item.Sum += recordMeasurement
			item.Min = min(item.Min, recordMeasurement)
			item.Max = max(item.Max, recordMeasurement)
		} else {
			results[strings.Clone(recordName)] = &WeaterStationData{
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
