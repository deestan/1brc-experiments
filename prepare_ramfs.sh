# Put on ram drive! Run as sudo
mkdir ramfs
mount -t ramfs -o SIZE=14GB ramfs ./ramfs && cp ./measurements.txt ./ramfs
