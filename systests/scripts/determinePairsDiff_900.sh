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

CURRENTDIR=`pwd`
BASENAME=`basename $CURRENTDIR`
systestsExpectedOutputs=/wres_share/testing/systestsExpectedOutputs/systests_expected_outputs/$BASENAME/output
#echo "$systestsExpectedOutputs"

# Comparing the dirListing.txt file.
if [ -f output/dirListing.txt -a -f $systestsExpectedOutputs/dirListing.txt ]
then
        echo "$echoPrefix Comparing listing with $systestsExpectedOutputs expected contents: diff -q output/dirListing.txt $systestsExpectedOutputs/dirListing.txt"
        diff -q output/dirListing.txt $systestsExpectedOutputs/dirListing.txt | tee /dev/stderr
fi

# For all files with "pairs.csv" in their name (could include pairs.csv or baseline_pairs.csv), but 
# without sorted in their names (in case an old sorted_pairs.csv is floating around), do...
for pairsFileName in $(ls output | grep pairs\.csv | grep -v sorted); do
    
  #if [ -f $systestsExpectedOutputs/sorted_$pairsFileName -a -f output/$pairsFileName ] # if both files exist
  if [ -f output/$pairsFileName ] # if pairs files exist
  then 
      echo "$echoPrefix Sorting and comparing file $pairsFileName with $systestsExpectedOutputs"
      # do sorting here
#echo "Ready to sort out the output/pairs.csv and compare the sorted output with $systestsExpectedOutputs/sorted_pairs.csv"
#    sort output/pairs.csv > output/sorted_pairs.csv
# sort the output/pairs.csv file with options -t, use a comman as delimiter; -k1s,1 column1 as directionary order 1st;
# -k4n,4 column 4 as numeric order 2nd; and -k2n,2 column 2 as numeric order 3rd
      sort -t, -k1d,1 -k4n,4 -k2n,2 output/$pairsFileName > output/sorted_$pairsFileName
      
      #diff --brief output/sorted_pairs.csv $systestsExpectedOutputs/sorted_pairs.csv 2>&1 | tee diff_sorted_pairs.txt # output the diffs with $systestsExpectedOutputs
      if [ -f $systestsExpectedOutputs/sorted_$pairsFileName -a -f output/sorted_$pairsFileName ] # if both files exist
      then
      	diff --brief output/sorted_$pairsFileName $systestsExpectedOutputs/sorted_$pairsFileName  | tee /dev/stderr
      elif [ ! -f output/$pairsFileName ]
      then
	echo "$echoPrefix Not comparing pairs file with $systestsExpectedOutputs: File output/$pairsFileName not found."
      elif [ ! -f $systestsExpectedOutputs/sorted_$pairsFileName ]
      then
	echo "$echoPrefix Not comparing pairs File with $systestsExpectedOutputs: $systestsExpectedOutputs/sorted_$pairsFileName not found."
      fi
  fi
done

# Comparing metric otuput .csv files that exist in both $systestsExpectedOutputs and outputs.
echo "$echoPrefix Comparing output .csv files with $systestsExpectedOutputs if the $systestsExpectedOutputs version exists..."
for csvFile in $(ls output | grep csv | grep -v pairs)
do
	if [ -f output/$csvFile -a -f $systestsExpectedOutputs/$csvFile ]
	then
		diff -q output/$csvFile $systestsExpectedOutputs/$csvFile | tee /dev/stderr
	fi
        # Hank, 3/9: I'm commenting this out.  I don't think we need to see when a csv file is not benchmarked.
        # If its not benchmarked, then we made a conscious decisioon not to check it.
        #elif [ ! -f benchmarks/$csvFile ]
	#then
                # echo "File benchmarks/$csvFile not found."
	#fi
done
