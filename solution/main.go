package main

import (
	"fmt"
	"hash/maphash"
	"os"
	"runtime"
	"runtime/pprof"
	"syscall"

	"golang.org/x/exp/mmap"
)

const mapInitSize uintptr = 10_000

type filePartition struct {
	start int
	end   int
}

type stationData struct {
	name          []byte
	min, max, sum float64
	count         int
}

type recordHandler = func([]byte, float64)
type statsMap = map[uint64]*stationData

func main() {
	if err := syscall.Setpriority(syscall.PRIO_PROCESS, 0, -20); err != nil {
		fmt.Fprintln(os.Stderr, "Not superuser: running process in default priority")
	}
	profFileName := os.Args[0] + ".prof"
	if os.Getenv("PROFILE") != "" {
		fmt.Fprintln(os.Stderr, "Profiling enabled")
		pfile, err := os.Create(profFileName)
		if err != nil {
			panic(err)
		}
		defer pfile.Close()
		pprof.StartCPUProfile(pfile)
		defer pprof.StopCPUProfile()
	}

	inputFile := "/tmpfs/measurements_1B.txt"
	if len(os.Args) > 1 {
		inputFile = os.Args[1]
	}
	fmt.Fprintln(os.Stderr, "Reading records from", inputFile)

	fileReader, err := mmap.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer fileReader.Close()
	partitions := partitionFile(fileReader)

	resultsCh := make(chan statsMap)
	hashSeed := maphash.MakeSeed()
	for _, partition := range partitions {
		go process(&partition, hashSeed, fileReader, resultsCh)
	}
	stats := make(statsMap, mapInitSize)
	for range partitions {
		statsPartition := <-resultsCh
		merge(stats, statsPartition)
	}
	for _, item := range stats {
		fmt.Printf("%s;%0.2f;%0.2f;%0.2f\n", item.name, item.max, item.min, item.sum/float64(item.count))
	}

	if os.Getenv("PROFILE") != "" {
		fmt.Fprintln(os.Stderr, "To use, run: go tool pprof", os.Args[0], profFileName)
	}
}

func merge(tgt statsMap, src statsMap) {
	for key, srcItem := range src {
		if tgtItem, ok := tgt[key]; ok {
			tgtItem.count += srcItem.count
			tgtItem.sum += srcItem.sum
			tgtItem.min = min(tgtItem.min, srcItem.min)
			tgtItem.max = max(tgtItem.max, srcItem.max)
		} else {
			tgt[key] = srcItem
		}
	}
}

func partitionFile(reader *mmap.ReaderAt) []filePartition {
	numPartitions := runtime.NumCPU()
	partitions := make([]filePartition, numPartitions)
	partitionSize := reader.Len() / numPartitions
	prevEnd := 0
	for i := range numPartitions {
		partition := filePartition{
			start: prevEnd,
			end:   prevEnd + partitionSize,
		}
		if i == numPartitions-1 {
			partition.end = reader.Len()
		} else {
			for reader.At(partition.end-1) != byte('\n') {
				partition.end += 1
			}
		}
		prevEnd = partition.end
		partitions[i] = partition
	}
	return partitions
}

func process(partition *filePartition, hashSeed maphash.Seed, reader *mmap.ReaderAt, resultCh chan statsMap) {
	stats := make(statsMap, mapInitSize)
	// The iterator pattern is a pleasant way to process data without allocating or copying
	iterateRecords(partition, reader, func(name []byte, measurement float64) {
		key := maphash.Bytes(hashSeed, name)
		if item, ok := stats[key]; ok {
			item.count += 1
			item.sum += measurement
			item.min = min(item.min, measurement)
			item.max = max(item.max, measurement)
		} else {
			nameCopy := make([]byte, len(name))
			copy(nameCopy, name)
			newItem := stationData{
				name:  nameCopy,
				min:   measurement,
				max:   measurement,
				sum:   measurement,
				count: 1,
			}
			stats[key] = &newItem
		}
	})
	resultCh <- stats
}

func iterateRecords(partition *filePartition, reader *mmap.ReaderAt, handler recordHandler) {
	pos := partition.start
	floatTempBuf := [6]byte{} // len("-99.99")==6
	var recordStart int
	var fieldSeparator int
	for pos < partition.end {
		for recordStart = pos; reader.At(pos) != ';'; pos++ {
		}
		for fieldSeparator = pos; reader.At(pos) != '\n'; pos++ {
		}
		measurementStart := fieldSeparator + 1
		name := make([]byte, fieldSeparator-recordStart)
		reader.ReadAt(name, int64(recordStart))
		floatBuf := floatTempBuf[:pos-measurementStart]
		reader.ReadAt(floatBuf, int64(measurementStart))
		measurement := trustingReadFloat64(floatBuf)
		handler(name, measurement)
		pos++
	}
}

// Lean hard on data source promising max 2 integral digits
// and at least one fractional digit
func trustingReadFloat64(data []byte) float64 {
	pos := 0
	negative := data[0] == '-'
	if negative {
		pos = 1
	}
	integral := float64(0)
	exponent := float64(0)
	for pos < len(data) {
		if data[pos] == '.' {
			pos += 1
			if pos == len(data)-2 {
				exponent = 100
			} else if pos == len(data)-1 {
				exponent = 10
			}
		}
		integral *= 10
		integral += float64(data[pos] - '0')
		pos += 1
	}
	if negative {
		integral = -integral
	}
	return integral / exponent
}
