package main

import (
	"os"
	"runtime"
	"syscall"
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

func NewMmapFile(filename string) (*MmapFile, error) {
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
		syscall.MAP_SHARED|syscall.MAP_POPULATE,
	)
	if err != nil {
		return nil, err
	}
	if err := syscall.Madvise(data, syscall.MADV_SEQUENTIAL|syscall.MADV_HUGEPAGE); err != nil {
		return nil, err
	}
	m := &MmapFile{Data: data}
	runtime.SetFinalizer(m, (*MmapFile).Close)
	return m, nil
}
