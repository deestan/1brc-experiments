package main

import "iter"

func Records(data []byte) iter.Seq[*Record] {
	return func(yield func(*Record) bool) {
		record := Record{}
		pos := 0
		var nameStart int
		for pos < len(data) {
			for nameStart = pos; data[pos] != ';'; pos++ {
			}
			record.Name = data[nameStart:pos]
			pos = parseMeasurement2(data, pos+1, &record.Measurement)
			if !yield(&record) {
				return
			}
		}
	}
}

func parseMeasurement2(data []byte, pos int, result *Decimal2) int {
	negative := data[pos] == '-'
	if negative {
		pos += 1
	}
	*result = 0
	for data[pos] != '\n' {
		if data[pos] == '.' {
			pos += 1
		}
		*result *= 10
		*result += int64(data[pos] - '0')
		pos += 1
	}
	// Number has either 1 or 2 fractional digits
	if data[pos-2] == '.' {
		*result *= 10
	}
	if negative {
		*result = -*result
	}
	return pos + 1
}
