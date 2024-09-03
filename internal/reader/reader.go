package reader

import (
	"iter"
	"unsafe"
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
		for pos < len(data) {
			pos = consumeName(data, pos, &record.Name)
			pos = consumeMeasurement(data, pos, &record.Measurement)
			if !yield(&record) {
				return
			}
		}
	}
}

type MutableString struct {
	Data unsafe.Pointer
	Len  int
}

func consumeName(data []byte, pos int, result *string) int {
	start := pos
	// Names are min length 1
	pos++
	for data[pos] != ';' {
		pos++
	}
	// It would be nice if the compiler optimized this instead :'(
	mutableResult := (*MutableString)(unsafe.Pointer(result))
	mutableResult.Data = unsafe.Pointer(&data[start])
	mutableResult.Len = pos - start
	return pos + 1
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
