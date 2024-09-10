# Run as sudo
chrt --fifo 99 /bin/time -vp ./solution ./ramfs/measurements.txt > /dev/null
