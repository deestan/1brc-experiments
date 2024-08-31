package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/alphadose/haxmap"
)

const readBufferSize int = 2 << 20 // 1MB
const mapInitSize uintptr = 10_000

type filePartition struct {
	filename string
	start    int64
	size     int64
}

type stationData struct {
	min, max, sum float64
	count         int
}

type recordHandler = func([]byte, float64)
type statsMap = *haxmap.Map[string, *stationData]

func main() {
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

	inputFile := os.Getenv("FILE")
	if inputFile == "" {
		inputFile = "../measurements_1B.txt"
	}

	partitions, err := partitionFile(inputFile)
	if err != nil {
		panic(err)
	}
	statsCh := make(chan statsMap)
	doneCh := make(chan error)
	for _, partition := range partitions {
		go process(&partition, statsCh, doneCh)
	}
	stats := haxmap.New[string, *stationData](mapInitSize)
	remaining := len(partitions)
	for remaining > 0 {
		select {
		case statsPartition := <-statsCh:
			merge(stats, statsPartition)
		case err := <-doneCh:
			if err != nil {
				panic(err)
			}
			remaining -= 1
		}
	}
	stats.ForEach(func(name string, item *stationData) bool {
		fmt.Printf("%s;%0.2f;%0.2f;%0.2f\n", name, item.max, item.min, item.sum/float64(item.count))
		return true
	})

	if os.Getenv("PROFILE") != "" {
		fmt.Fprintln(os.Stderr, "To use, run: go tool pprof", os.Args[0], profFileName)
	}
}

func merge(tgt statsMap, src statsMap) {
	src.ForEach(func(name string, srcItem *stationData) bool {
		if tgtItem, ok := tgt.Get(name); ok {
			tgtItem.count += srcItem.count
			tgtItem.sum += srcItem.sum
			tgtItem.min = min(tgtItem.min, srcItem.min)
			tgtItem.max = max(tgtItem.max, srcItem.max)
		} else {
			tgt.Set(name, srcItem)
		}
		return true
	})
}

func partitionFile(filename string) ([]filePartition, error) {
	fi, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()
	numPartitions := int64(runtime.NumCPU())
	partitions := make([]filePartition, numPartitions)
	partitionSize := fileSize / numPartitions
	for i := range numPartitions {
		partitions[i] = filePartition{
			filename: filename,
			start:    i * partitionSize,
			size:     partitionSize,
		}
		if i == numPartitions-1 {
			partitions[i].size = fileSize - partitions[i].start
		}
	}
	return partitions, nil
}

func process(partition *filePartition, statsCh chan statsMap, doneCh chan error) {
	stats := haxmap.New[string, *stationData](mapInitSize)
	// The iterator pattern is a pleasant way to process data without allocating or copying
	err := iterateRecords(partition, func(stationName []byte, measurement float64) {
		name := string(stationName)
		if item, ok := stats.Get(name); ok {
			item.count += 1
			item.sum += measurement
			item.min = min(item.min, measurement)
			item.max = max(item.max, measurement)
		} else {
			newItem := stationData{
				min:   measurement,
				max:   measurement,
				sum:   measurement,
				count: 1,
			}
			stats.Set(name, &newItem)
		}
	})
	if err != nil {
		doneCh <- err
		return
	}

	statsCh <- stats
	doneCh <- nil
}

func iterateRecords(partition *filePartition, handler recordHandler) error {
	f, err := os.Open(partition.filename)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(partition.start, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, readBufferSize)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(buf, 0)
	scanner.Split(bufio.ScanLines)
	remaining := partition.size

	// Each partition processor reads one record past its limit,
	// so each subsequent processor must skip the first record delimiter.
	// This adjusts for records that cross partitions.
	if partition.start != 0 {
		scanner.Scan()
		remaining -= int64(len(scanner.Bytes()) + 1)
	}

	for scanner.Scan() {
		data := scanner.Bytes()
		splitPos := bytes.Index(data, []byte{';'})
		handler(data[:splitPos], trustingReadFloat64(data[splitPos+1:]))
		remaining -= int64(len(scanner.Bytes()) + 1)
		if remaining < 0 {
			break
		}
	}

	return nil
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
