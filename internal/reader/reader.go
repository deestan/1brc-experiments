package reader

import (
	"iter"
)

type Decimal1 = int64

type Record struct {
	Name        []byte
	Measurement Decimal1
}

var DIGITS_1 = [58]int64{
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
}

var DIGITS_10 = [58]int64{
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 10, 20, 30, 40, 50, 60, 70, 80, 90,
}
var DIGITS_100 = [58]int64{
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 100, 200, 300, 400, 500, 600, 700, 800, 900,
}

func Records(data []byte) iter.Seq[*Record] {
	return func(yield func(*Record) bool) {
		record := Record{}
		pos := 0
		var nameStart int
		for pos < len(data) {
			for nameStart = pos; data[pos] != ';'; pos++ {
			}
			record.Name = data[nameStart:pos]
			pos = consumeMeasurement(data, pos+1, &record.Measurement)
			if !yield(&record) {
				return
			}
		}
	}
}

func consumeMeasurement(data []byte, pos int, result *Decimal1) int {
	negative := data[pos] == '-'
	if negative {
		pos += 1
	}
	// Numbers are either length 3 or 4, ie. x.x or xx.x
	length3 := data[pos+3] == '\n'
	// If not competition code, should check and error on (!length3 && data[pos+4] != '\n')
	if length3 {
		*result = DIGITS_10[data[pos]] + DIGITS_1[data[pos+2]]
	} else {
		*result = DIGITS_100[data[pos]] + DIGITS_10[data[pos+1]] + DIGITS_1[data[pos+3]]
	}
	if negative {
		*result = -*result
	}
	if length3 {
		return pos + 4
	} else {
		return pos + 5
	}
}

func Decimal1ToFloat64(dec Decimal1) float64 {
	return float64(dec) / 10
}
