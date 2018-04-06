#!/bin/sh
prefix="/Volumes/datasets/ITCH/bin/S"
suffix="-v41.txt"
for file in /Volumes/datasets/ITCH/bin/S07*13*.txt
do
    string=${file#$prefix}
    string=${string%$suffix}
    python unpack_txt.py $string
done
