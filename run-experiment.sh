set -e
go build cmd/experiment/*
./experiment $*
go tool pprof -top ./experiment ./experiment.prof
