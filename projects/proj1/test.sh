#!/bin/sh

# *4M
file_sizes=(1 16 64 256 1024)
block_sizes=(1 512 1024 2048 4096 65536)

for file_size in $file_sizes; do
    echo "file_size $file_size"
    dd if=/dev/random of=$file_size bs=4M count=$file_size
    for block_size in $block_sizes; do
        echo "block_size $block_size"
        ./test_file.sh $file_size $block_size
    done
    rm $file_size
    echo
done
