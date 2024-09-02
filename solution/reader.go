package main

import (
	"iter"
)

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
var DIGITS_1000 = [58]int64{
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
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
			pos = consumeMeasurement2(data, pos+1, &record.Measurement)
			if !yield(&record) {
				return
			}
		}
	}
}

func consumeMeasurement2(data []byte, pos int, result *Decimal2) int {
	negative := data[pos] == '-'
	if negative {
		pos += 1
	}
	fieldEnd := pos + 3
	for ; data[fieldEnd] != '\n'; fieldEnd++ {
	}
	switch fieldEnd - pos {
	case 3: // x.x
		*result = DIGITS_100[data[pos]] + DIGITS_10[data[pos+2]]
	case 4: // x.xx or xx.x
		if data[1] == '.' { // x.xx
			*result = DIGITS_100[data[pos]] + DIGITS_100[data[pos+2]] + DIGITS_1[data[pos+3]]
		} else { // xx.x
			*result = DIGITS_1000[data[pos]] + DIGITS_100[data[pos+1]] + DIGITS_10[data[pos+3]]
		}
	case 5: // xx.xx
		*result = DIGITS_1000[data[pos]] + DIGITS_100[data[pos+1]] + DIGITS_10[data[pos+3]] + DIGITS_1[data[pos+4]]
	}
	if negative {
		*result = -*result
	}
	return fieldEnd + 1
}
