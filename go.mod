module github.com/deestan/1brc-go

go 1.23.0

require github.com/bytedance/gopkg v0.1.1

require internal/mmap v0.0.0

replace internal/mmap => ./internal/mmap

require internal/reader v0.0.0

require golang.org/x/sys v0.19.0 // indirect

replace internal/reader => ./internal/reader
