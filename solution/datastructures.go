package main

// Represents a value rounded to two decimal digits
type Decimal2 = int64

type Record struct {
	Name        []byte
	Measurement Decimal2
}

func Decimal2ToFloat64(dec Decimal2) float64 {
	return float64(dec) / 100
}
