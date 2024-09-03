package reader

import (
	"iter"
)

type Record struct {
	Name        string
	Measurement Decimal1
}

type Decimal1 = int64

func Records(data []byte) iter.Seq[*Record] {
	return func(yield func(*Record) bool) {
		record := Record{}
		pos := 0
		var nameStart int
		for pos < len(data) {
			for nameStart = pos; data[pos] != ';'; pos++ {
			}
			record.Name = string(data[nameStart:pos])
			pos = consumeMeasurement(data, pos+1, &record.Measurement)
			if !yield(&record) {
				return
			}
		}
	}
}

func consumeMeasurement(data []byte, pos int, result *Decimal1) int {
	negativizer := int64(0)
	if data[pos] == '-' {
		pos += 1
		negativizer = -1
	}
	// Numbers are either length 3 or 4, ie. x.x or xx.x
	length3 := data[pos+3] == '\n'
	if length3 {
		*result = (10*int64(data[pos]-'0') + int64(data[pos+2]-'0')) | negativizer
		return pos + 4
	} else {
		// If not competition code, should check and error on (data[pos+4] != '\n')
		*result = (100*int64(data[pos]-'0') + 10*int64(data[pos+1]-'0') + int64(data[pos+3]-'0')) | negativizer
		return pos + 5
	}
}

func Decimal1ToFloat64(dec Decimal1) float64 {
	return float64(dec) / 10
}
