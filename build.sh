set -e
PROF=
if [ -f ./solution.prof ]; then
    echo Compiling with pgo
    PROF="-pgoprofile ./solution.prof"
fi
rm -f ./solution
go build -gcflags "$PROF -d=inlfuncswithclosures -d=inlstaticinit -d=inlbudgetslack=100000 -d=alignhot -d pgoinline -d=pgoinlinebudget=100000 -d pgodevirtualize -d disablenil" -o ./solution ./cmd/solution/*.go
