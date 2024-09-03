set -e
rm -f ./solution ./solution.prof
go build -o solution cmd/solution/main.go
PROFILE=yep ./solution $1 > /dev/null
go tool pprof -top ./solution ./solution.prof | head -n 20
