package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
)

const readBufferSize int = 2 << 20 // 1MB

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

func main() {
	if os.Getenv("PROFILE") != "" {
		profFileName := os.Args[0] + ".prof"
		fmt.Println("Profiling to", profFileName)
		fmt.Println("To use, run: go tool pprof", os.Args[0], profFileName)
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
	statsCh := make(chan map[string]*stationData)
	doneCh := make(chan error)
	for _, partition := range partitions {
		go process(&partition, statsCh, doneCh)
	}
	stats := map[string]*stationData{}
	remaining := len(partitions)
	for remaining > 0 {
		select {
		case statsPartition := <-statsCh:
			for inName, inStat := range statsPartition {
				if stat, ok := stats[inName]; ok {
					stat.count += inStat.count
					stat.sum += inStat.sum
					stat.min = min(stat.min, inStat.min)
					stat.max = max(stat.max, inStat.max)
				} else {
					stats[inName] = inStat
				}
			}
		case err := <-doneCh:
			if err != nil {
				panic(err)
			}
			remaining -= 1
		}
	}
	for name := range stats {
		stat := stats[name]
		fmt.Printf("%s;%0.2f;%0.2f;%0.2f\n", name, stat.max, stat.min, stat.sum/float64(stat.count))
	}
}

func partitionFile(filename string) ([]filePartition, error) {
	fi, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()
	numPartitions := int64(10 * runtime.NumCPU())
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

func process(partition *filePartition, statsCh chan map[string]*stationData, doneCh chan error) {
	stats := map[string]*stationData{}
	// The iterator pattern is a pleasant way to process data without allocating or copying
	err := iterateRecords(partition, func(stationName []byte, measurement float64) {
		key := string(stationName)
		if station, ok := stats[key]; ok {
			station.sum += measurement
			station.min = min(station.min, measurement)
			station.max = max(station.max, measurement)
			station.count += 1
		} else {
			stats[key] = &stationData{measurement, measurement, measurement, 1}
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
		if err := parseRecord(data, handler); err != nil {
			return err
		}
		remaining -= int64(len(scanner.Bytes()) + 1)
		if remaining < 0 {
			break
		}
	}

	return nil
}

func parseRecord(data []byte, handler recordHandler) error {
	splitPos := bytes.Index(data, []byte{';'})
	name := data[:splitPos]
	val, err := strconv.ParseFloat(string(data[splitPos+1:]), 64)
	if err != nil {
		return err
	}
	handler(name, val)
	return nil
}
