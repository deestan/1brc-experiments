# Put on ram drive! Run as sudo
set -e
mkdir -p ramfs
mount -t tmpfs -omode=1777,huge=always,noswap tmpfs ./ramfs
cp ./measurements.txt ./ramfs
