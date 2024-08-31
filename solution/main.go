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
	namePos       int
	nameLen       int
	min, max, sum int64
	count         int
}

type recordHandler = func(int, int, int64)
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
	nameBuf := make([]byte, 101)
	for _, item := range stats {
		name := nameBuf[:item.nameLen]
		fileReader.ReadAt(name, int64(item.namePos))
		mMax := measurement2ToFloat64(item.max)
		mMin := measurement2ToFloat64(item.min)
		mAvg := measurement2ToFloat64(item.sum) / float64(item.count)
		fmt.Printf("%s;%0.2f;%0.2f;%0.2f\n", name, mMax, mMin, mAvg)
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
	hasher := maphash.Hash{}
	hasher.SetSeed(hashSeed)
	// The iterator pattern is a pleasant way to process data without allocating or copying
	iterateRecords(partition, reader, func(namePos int, nameLen int, measurement int64) {
		hasher.Reset()
		for i := range nameLen {
			hasher.WriteByte(reader.At(namePos + i))
		}
		key := hasher.Sum64()
		if item, ok := stats[key]; ok {
			item.count += 1
			item.sum += measurement
			item.min = min(item.min, measurement)
			item.max = max(item.max, measurement)
		} else {
			newItem := stationData{
				namePos: namePos,
				nameLen: nameLen,
				min:     measurement,
				max:     measurement,
				sum:     measurement,
				count:   1,
			}
			stats[key] = &newItem
		}
	})
	resultCh <- stats
}

func iterateRecords(partition *filePartition, reader *mmap.ReaderAt, handler recordHandler) {
	pos := partition.start
	var nameStart int
	var nameLen int
	var measurement int64
	for pos < partition.end {
		for nameStart = pos; reader.At(pos) != ';'; pos++ {
		}
		nameLen = pos - nameStart
		pos = consumeMeasurement2(reader, pos+1, &measurement)
		handler(nameStart, nameLen, measurement)
	}
}

// Value of the measurement, multiplied by 10^2
func consumeMeasurement2(reader *mmap.ReaderAt, start int, result *int64) int {
	pos := start
	negative := reader.At(pos) == '-'
	if negative {
		pos += 1
	}
	*result = 0
	for reader.At(pos) != '\n' {
		if reader.At(pos) == '.' {
			pos += 1
		}
		*result *= 10
		*result += int64(reader.At(pos) - '0')
		pos += 1
	}
	// Number has either 1 or 2 fractional digits
	if reader.At(pos-2) == '.' {
		*result *= 10
	}
	if negative {
		*result = -*result
	}
	return pos + 1
}

func measurement2ToFloat64(measurement int64) float64 {
	return float64(measurement) / 100
}
