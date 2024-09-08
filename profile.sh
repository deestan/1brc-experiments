set -e
./build.sh
rm -f ./solution.prof
PROFILE=yep ./solution $1 > /dev/null
go tool pprof -top ./solution ./solution.prof | head -n 20
