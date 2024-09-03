rm ./solution
go build cmd/solution/solution.go
PROFILE=yep ./solution $1 > /dev/null
go tool pprof -top ./solution ./solution.prof | head -n 20
