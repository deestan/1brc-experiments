package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/bytedance/gopkg/util/xxhash3"
)

func doTest() {
	stuffs := [100][]byte{}
	for i := range len(stuffs) {
		stuffs[i] = []byte(time.Now().UTC().Format(time.RFC3339Nano))
	}
	fmt.Println(os.Args)
	if len(os.Args) > 1 && os.Args[1] == "1" {
		doTest1(stuffs)
	} else {
		doTest2(stuffs)
	}
}

func doTest1(stuffs [100][]byte) {
	fmt.Println("test 1")
	s := make(map[string]int, 100)
	sum := 0
	for i := range 1_000_000_000 {
		key := string(stuffs[i%len(stuffs)])
		if v, ok := s[key]; ok {
			s[key] = v + 1
			sum += v
		} else {
			s[key] = 1
		}
	}
	fmt.Println(sum)
}

type Item struct {
	id  [2]uint64
	val int
}

type LookupKey = uint16

func doTest2(stuffs [100][]byte) {
	fmt.Println("test 2")
	s := make(map[LookupKey][]Item, 100)
	sum := 0
out:
	for i := range 1_000_000_000 {
		idKey := xxhash3.Hash128(stuffs[i%len(stuffs)])
		lookupKey := LookupKey(idKey[1])
		itemArr, ok := s[lookupKey]
		if !ok {
			newArr := make([]Item, 1)[:1]
			newArr[0].id = idKey
			newArr[0].val = 1
			s[lookupKey] = newArr
			continue out
		}
		for i := range itemArr {
			item := &itemArr[i]
			if item.id == idKey {
				item.val += 1
				sum += item.val
				continue out
			}
		}
		var grownArr []Item
		if cap(itemArr) == len(itemArr) {
			grownArr = make([]Item, len(itemArr)+1)
			copy(grownArr, itemArr)
		} else {
			grownArr = itemArr[:len(itemArr)+1]
		}
		item := &grownArr[len(itemArr)]
		item.id = idKey
		item.val = 1
		s[lookupKey] = grownArr
	}
	fmt.Println(sum)
}

func main() {
	profFileName := os.Args[0] + ".prof"
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
	doTest()
}
