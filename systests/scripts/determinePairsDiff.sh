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

# If BENCHMARK_DIR is set, use it, else use "benchmarks", e.g. for scenario9*
benchmarkDir="${BENCHMARK_DIR:-benchmarks}"
outputDirPrefix=wres_evaluation_output_


# Comparing the dirListing.txt file.
if [ -f ${outputDirPrefix}*/dirListing.txt -a -f ${benchmarkDir}/dirListing.txt ]
then
        echo "$echoPrefix Comparing listing with benchmark expected contents: diff -q ${outputDirPrefix}*/dirListing.txt ${benchmarkDir}/dirListing.txt"
        diff -q ${outputDirPrefix}*/dirListing.txt ${benchmarkDir}/dirListing.txt | tee /dev/stderr
fi

# For all files with "pairs.csv" in their name (could include pairs.csv or baseline_pairs.csv), but 
# without sorted in their names (in case an old sorted_pairs.csv is floating around), do...
for directory in ${outputDirPrefix}*
do
    for pairsFile in $directory/pairs.csv
    do
        echo "$echoPrefix Sorting and comparing file $pairsFile with benchmark..."
        pairsFileBaseName=$( basename $pairsFile )
        # do sorting here
        sort -t, -k1d,1 -k4n,4 -k2n,2 $pairsFile > $directory/sorted_$pairsFileBaseName

        if [ -f ${benchmarkDir}/sorted_$pairsFileBaseName \
             -a -f $directory/sorted_$pairsFileBaseName ]
        then
            # both files exist, do the comparison
            diff --brief $directory/sorted_$pairsFileBaseName ${benchmarkDir}/sorted_$pairsFileBaseName  | tee /dev/stderr
        elif [ ! -f $directory/$pairsFileBaseName ]
        then
	        echo "$echoPrefix Not comparing pairs file with benchmark: File $directory/$pairsFileBaseName not found."
        elif [ ! -f ${benchmarkDir}/sorted_$pairsFileBaseName ]
        then
	        echo "$echoPrefix Not comparing pairs File with benchmark: ${benchmarkDir}/sorted_$pairsFileBaseName not found."
        fi
    done
done

# Comparing metric otuput .csv files that exist in both ${benchmarkDir} and outputs.
echo "$echoPrefix Comparing output .csv files with ${benchmarkDir} if the benchmark version exists..."
for csvFile in $(ls ${outputDirPrefix}* | grep csv | grep -v pairs)
do
	if [ -f ${outputDirPrefix}*/$csvFile -a -f ${benchmarkDir}/$csvFile ]
	then
		diff -q ${outputDirPrefix}*/$csvFile ${benchmarkDir}/$csvFile | tee /dev/stderr
	fi
        # Hank, 3/9: I'm commenting this out.  I don't think we need to see when a csv file is not benchmarked.
        # If its not benchmarked, then we made a conscious decisioon not to check it.
        #elif [ ! -f ${benchmarkDir}/$csvFile ]
	#then
                # echo "File ${benchmarkDir}/$csvFile not found."
	#fi
done
