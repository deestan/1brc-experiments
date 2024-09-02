rm ./1brc-helge
go build
nice -n 10 ionice -c 1 -n 1 /bin/time -p ./1brc-helge /ramfs/measurements_1B.txt > /dev/null
