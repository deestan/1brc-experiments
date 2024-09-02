package main

import (
	"iter"
)

var DIGITS = [58]int64{
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
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
		*result = DIGITS[data[pos]]*100 + DIGITS[data[pos+2]]*10
	case 4: // x.xx or xx.x
		if data[1] == '.' { // x.xx
			*result = DIGITS[data[pos]]*100 + DIGITS[data[pos+2]]*100 + DIGITS[data[pos+3]]
		} else { // xx.x
			*result = DIGITS[data[pos]]*1000 + DIGITS[data[pos+1]]*100 + DIGITS[data[pos+3]]*10
		}
	case 5: // xx.xx
		*result = DIGITS[data[pos]]*1000 + DIGITS[data[pos+1]]*100 + DIGITS[data[pos+3]]*10 + DIGITS[data[pos+4]]
	}
	if negative {
		*result = -*result
	}
	return fieldEnd + 1
}
