package main

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

type MmapFile struct {
	Data []byte
}

func (m *MmapFile) Close() error {
	data := m.Data
	m.Data = nil
	runtime.SetFinalizer(m, nil)
	return syscall.Munmap(data)
}

type MmapAlloc[T any] struct {
	v    *T
	data []byte
}

func (m *MmapAlloc[T]) Close() error {
	datax := m.data
	m.data = nil
	runtime.SetFinalizer(m, nil)
	return syscall.Munmap(datax)
}

func Alloc[T any](size int64) (*T, error) {
	data, err := syscall.Mmap(
		-1,
		0,
		int(size),
		syscall.PROT_WRITE|syscall.PROT_READ,
		syscall.MAP_PRIVATE|syscall.MAP_HUGETLB|syscall.MAP_ANONYMOUS,
	)
	if err != nil {
		return nil, err
	}
	v := (*T)(unsafe.Pointer(&data[0]))
	m := &MmapAlloc[T]{v: v, data: data}
	runtime.SetFinalizer(m, (*MmapAlloc[T]).Close)
	return m.v, nil
}

func NewMmapFile(filename string, pad int) (*MmapFile, error) {
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
		int(size)+pad,
		syscall.PROT_READ,
		syscall.MAP_SHARED|syscall.MAP_POPULATE,
	)
	if err != nil {
		return nil, err
	}
	if err := syscall.Madvise(data, syscall.MADV_SEQUENTIAL|syscall.MADV_HUGEPAGE); err != nil {
		return nil, err
	}
	m := &MmapFile{Data: data[:len(data)-pad]}
	runtime.SetFinalizer(m, (*MmapFile).Close)
	return m, nil
}
