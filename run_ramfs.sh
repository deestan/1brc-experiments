# Run as sudo
nice -n 10 ionice -c 1 -n 1 /bin/time -p ./solution ./ramfs/measurements.txt > /dev/null
