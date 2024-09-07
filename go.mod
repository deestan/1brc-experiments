module github.com/deestan/1brc-go

go 1.23.0

require github.com/bytedance/gopkg v0.1.1

require internal/mmap v0.0.0

replace internal/mmap => ./internal/mmap

require internal/reader v0.0.0

require (
	gitee.com/menciis/gkit v0.0.0-20240704103244-d3f65ed26d21 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/sys v0.19.0 // indirect
)

replace internal/reader => ./internal/reader
