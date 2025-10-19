#!/bin/sh

if [[ $# != 2 ]]; then
    echo "usage $0 filename blocksize"
    echo "sudo password may be required for flushing cache"
    exit -1
fi

sudo echo -n

echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
time ./block_test $1 $2 read random
echo

echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
time ./block_test $1 $2 read seq
echo

echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
time ./block_test $1 $2 mmap random
echo

echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
time ./block_test $1 $2 mmap seq
echo

