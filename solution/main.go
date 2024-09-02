package main

import (
	"fmt"
	"hash/maphash"
	"os"
	"runtime"
	"runtime/pprof"
)

const maxStations uintptr = 10_000

type weaterStationData struct {
	min, max, sum int64
	name          []byte
	count         int
}
type weatherStationKey = uint64
type processedResults = map[weatherStationKey]*weaterStationData

func main() {
	if os.Getenv("PROFILE") != "" {
		profFileName := os.Args[0] + ".prof"
		fmt.Fprintln(os.Stderr, "### Profiling enabled")
		pfile, err := os.Create(profFileName)
		if err != nil {
			panic(err)
		}
		defer pfile.Close()
		pprof.StartCPUProfile(pfile)
		defer func() {
			pprof.StopCPUProfile()
			fmt.Fprintln(os.Stderr, "### Profiling done, run:\ngo tool pprof", os.Args[0], profFileName)
		}()
	}

	inputFile := "../measurements_1B.txt"
	if len(os.Args) > 1 {
		inputFile = os.Args[1]
	}
	fmt.Fprintln(os.Stderr, "Reading records from", inputFile)

	fileMap, err := NewMmapFile(inputFile)
	if err != nil {
		panic(err)
	}
	defer fileMap.Close()

	stats := processParallel(fileMap.data)
	for _, item := range stats {
		mMax := Decimal1ToFloat64(item.max)
		mMin := Decimal1ToFloat64(item.min)
		mAvg := Decimal1ToFloat64(item.sum) / float64(item.count)
		fmt.Printf("%s;%0.2f;%0.2f;%0.2f\n", item.name, mMax, mMin, mAvg)
	}
}

func processParallel(data []byte) processedResults {
	hashSeed := maphash.MakeSeed()
	partitions := partitionData(data, runtime.NumCPU())
	resultsCh := make(chan processedResults)
	for _, partition := range partitions {
		go process(hashSeed, partition, resultsCh)
	}
	stats := make(processedResults, maxStations)
	for range partitions {
		merge(stats, <-resultsCh)
	}
	return stats
}

func merge(tgt processedResults, src processedResults) {
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

func partitionData(data []byte, numPartitions int) [][]byte {
	partitions := make([][]byte, numPartitions)
	partitionSize := len(data) / numPartitions
	prevEnd := 0
	for i := range numPartitions {
		start := prevEnd
		end := max(start, partitionSize*(i+1))
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

func process(hashSeed maphash.Seed, data []byte, resultCh chan processedResults) {
	results := make(processedResults, maxStations)
	for record := range Records(data) {
		key := maphash.Bytes(hashSeed, record.Name)
		if item, ok := results[key]; ok {
			item.count += 1
			item.sum += record.Measurement
			item.min = min(item.min, record.Measurement)
			item.max = max(item.max, record.Measurement)
		} else {
			nameCopy := make([]byte, len(record.Name))
			copy(nameCopy, record.Name)
			newItem := weaterStationData{
				name:  nameCopy,
				min:   record.Measurement,
				max:   record.Measurement,
				sum:   record.Measurement,
				count: 1,
			}
			results[key] = &newItem
		}
	}
	resultCh <- results
}
