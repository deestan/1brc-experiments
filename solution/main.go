package main

import (
	"fmt"
	"hash/maphash"
	"os"
	"runtime"
	"runtime/pprof"
	"syscall"
)

const mapInitSize uintptr = 10_000

type stationData struct {
	name          []byte
	min, max, sum int64
	count         int
}

type mmapData struct {
	data []byte
}

func (m *mmapData) close() error {
	data := m.data
	m.data = nil
	runtime.SetFinalizer(m, nil)
	return syscall.Munmap(data)
}

type recordHandler = func([]byte, int64)
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

	fileMap, err := openMmap(inputFile)
	if err != nil {
		panic(err)
	}
	defer fileMap.close()
	partitions := partitionData(fileMap.data)

	resultsCh := make(chan statsMap)
	hashSeed := maphash.MakeSeed()
	for _, partition := range partitions {
		go process(hashSeed, partition, resultsCh)
	}
	stats := make(statsMap, mapInitSize)
	for range partitions {
		statsPartition := <-resultsCh
		merge(stats, statsPartition)
	}
	for _, item := range stats {
		mMax := measurement2ToFloat64(item.max)
		mMin := measurement2ToFloat64(item.min)
		mAvg := measurement2ToFloat64(item.sum) / float64(item.count)
		fmt.Printf("%s;%0.2f;%0.2f;%0.2f\n", item.name, mMax, mMin, mAvg)
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

func partitionData(data []byte) [][]byte {
	numPartitions := runtime.NumCPU()
	partitions := make([][]byte, numPartitions)
	partitionSize := len(data) / numPartitions
	prevEnd := 0
	for i := range numPartitions {
		start := prevEnd
		end := partitionSize * (i + 1)
		if i == numPartitions-1 {
			end = len(data)
		} else {
			for data[end-1] != byte('\n') && end < len(data) {
				end += 1
			}
		}
		prevEnd = end
		partitions[i] = data[start:end]
	}
	return partitions
}

func openMmap(filename string) (*mmapData, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	data, err := syscall.Mmap(
		int(f.Fd()),
		0,
		int(size),
		syscall.PROT_READ,
		syscall.MAP_PRIVATE|syscall.MAP_POPULATE,
	)
	if err != nil {
		return nil, err
	}
	m := &mmapData{data: data}
	runtime.SetFinalizer(m, (*mmapData).close)
	return m, nil
}

func process(hashSeed maphash.Seed, data []byte, resultCh chan statsMap) {
	stats := make(statsMap, mapInitSize)
	// The iterator pattern is a pleasant way to process data without allocating or copying
	iterateRecords(data, func(name []byte, measurement int64) {
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

func iterateRecords(data []byte, handler recordHandler) {
	pos := 0
	var nameStart int
	var measurement int64
	var name []byte
	for pos < len(data) {
		for nameStart = pos; data[pos] != ';'; pos++ {
		}
		name = data[nameStart:pos]
		pos = consumeMeasurement2(data, pos+1, &measurement)
		handler(name, measurement)
	}
}

// Value of the measurement, multiplied by 10^2
func consumeMeasurement2(data []byte, start int, result *int64) int {
	pos := start
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

func measurement2ToFloat64(measurement int64) float64 {
	return float64(measurement) / 100
}
