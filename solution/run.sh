rm ./1brc-helge
go build
/bin/time -p ./1brc-helge /ramfs/measurements_1B.txt > /dev/null
