package main

import (
	"fmt"
	"internal/mmap"
	"internal/reader"
	"os"
	"runtime"
	"runtime/pprof"
)

const maxStations uintptr = 10_000

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

	inputFile := "measurements.txt"
	if len(os.Args) > 1 {
		inputFile = os.Args[1]
	}
	fmt.Fprintln(os.Stderr, "Reading records from", inputFile)

	fileMap, err := mmap.NewMmapFile(inputFile)
	if err != nil {
		panic(err)
	}
	defer fileMap.Close()

	stats := processParallel(fileMap.Data)
	for name, item := range stats {
		mMax := reader.Decimal1ToFloat64(item.Max)
		mMin := reader.Decimal1ToFloat64(item.Min)
		mAvg := reader.Decimal1ToFloat64(item.Sum) / float64(item.Count)
		fmt.Printf("%s;%0.1f;%0.1f;%0.1f\n", name, mMax, mMin, mAvg)
	}
}

func processParallel(data []byte) reader.ProcessedResults {
	partitions := partitionData(data, runtime.NumCPU())
	resultsCh := make(chan reader.ProcessedResults)
	for _, partition := range partitions {
		go process(partition, resultsCh)
	}
	stats := make(reader.ProcessedResults, maxStations)
	for range partitions {
		merge(stats, <-resultsCh)
	}
	return stats
}

func merge(tgt reader.ProcessedResults, src reader.ProcessedResults) {
	for key, srcItem := range src {
		if tgtItem, ok := tgt[key]; ok {
			tgtItem.Count += srcItem.Count
			tgtItem.Sum += srcItem.Sum
			tgtItem.Min = min(tgtItem.Min, srcItem.Min)
			tgtItem.Max = max(tgtItem.Max, srcItem.Max)
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

func process(data []byte, resultCh chan reader.ProcessedResults) {
	results := make(reader.ProcessedResults, maxStations)
	reader.IterInto(data, results)
	resultCh <- results
}
