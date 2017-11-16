#!/bin/bash

# Description: if output/pairs.csv and benchmarks/sorted_pairs.csv both exist, then sort out the output/pairs.csv
# And find the differents with benchmarks/sorted_pairs.csv, pipe the output to /output/diff_sorted_pairs.txt 

# Usage: $0 test_dir
# Author: Raymond.Chui@***REMOVED***
# Created: November, 2017

# Copied from runtest.sh; this must be kept consistent.  
echoPrefix="===================="

if [ $# -gt 0 ] # if a dictory is provided
then
	test_dir=$1
	if [ ! -d $test_dir ]
	then
        	echo "Directory $test_dir not found."
        	exit 2
	fi
	cd $test_dir
fi

if [ -f benchmarks/sorted_pairs.csv -a -f output/pairs.csv ] # if both files exist
then 
    echo "$echoPrefix Sorting and comparing pairs file with benchmark..."
    # do sorting here
#echo "Ready to sort out the output/pairs.csv and compare the sorted output with benchmarks/sorted_pairs.csv"
    sort output/pairs.csv > output/sorted_pairs.csv
    #diff --brief output/sorted_pairs.csv benchmarks/sorted_pairs.csv 2>&1 | tee diff_sorted_pairs.txt # output the diffs with benchmarks
    diff --brief output/sorted_pairs.csv benchmarks/sorted_pairs.csv  | tee /dev/stderr
elif [ ! -f output/pairs.csv ]
then
	echo "$echoPrefix Not comparing pairs file with benchmark: File output/pairs.csv not found."
elif [ ! -f benchmarks/sorted_pairs.csv ]
then
	echo "$echoPrefix Not comparing pairs File with benchmark: benchmarks/sorted_pairs.csv not found."
fi
