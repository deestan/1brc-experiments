# Put on ram drive! Run as sudo
set -e
mkdir -p ramfs
mount -t ramfs -o SIZE=14GB ramfs ./ramfs && cp ./measurements.txt ./ramfs
