#!/bin/bash

# Description: if output/pairs.csv and benchmarks/sorted_pairs.csv both exist, then sort out the output/pairs.csv
# And find the differents with benchmarks/sorted_pairs.csv, pipe the output to /output/diff_sorted_pairs.txt 

# Usage: $0 test_dir
# Author: Raymond.Chui@***REMOVED***
# Created: November, 2017

# Copied from runtest.sh; this must be kept consistent.
echoPrefix="===================="

PASSED_RESULT=0
OUTPUT_NOT_FOUND_RESULT=2
SORT_FAILED_RESULT=4
DIFFERENT_FILES_RESULT=8
DIFFERENT_PAIRS_RESULT=16
DIFFERENT_CSVS_RESULT=32

# Assume passage until failure encountered, add failures into this.
testResult=${PASSED_RESULT}

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

# ================================================
# Comparing the dirListing.txt file
# ================================================
if [ -f ${outputDirPrefix}*/dirListing.txt -a -f ${benchmarkDir}/dirListing.txt ]
then
    echo "$echoPrefix Comparing listing with benchmark expected contents: diff -q ${outputDirPrefix}*/dirListing.txt ${benchmarkDir}/dirListing.txt"
    # Avoid pipe to "tee" because it hides the exit code of "diff"
    diff -w -q ${outputDirPrefix}*/dirListing.txt ${benchmarkDir}/dirListing.txt
    diffResult=$?
    if [ ${diffResult} -ne 0 ]
    then
        echo "$echoPrefix The dirListing.txt file does not match benchmark." | tee /dev/stderr
        testResult=$(( ${testResult} + ${DIFFERENT_FILES_RESULT} ))
    fi
fi

# ================================================
# Check that output dir exists
# ================================================
ls ${outputDirPrefix}* &>/dev/null
lsResult=$?

if [ $lsResult -ne 0 ]
then
    echo "$echoPrefix No output directory created." | tee /dev/stderr
    testResult=$(( ${testResult} + ${OUTPUT_NOT_FOUND_RESULT} ))
fi

# ================================================
# Check the pairs files
# ================================================

# For all files with "pairs.csv" in their name (could include pairs.csv or baseline_pairs.csv), but 
# without sorted in their names (in case an old sorted_pairs.csv is floating around), do...
for directory in ${outputDirPrefix}*
do
	if [ -f $directory/pairs.csv ]
	then
    	for pairsFile in $directory/pairs.csv
    	do
        	echo "$echoPrefix Sorting and comparing file $pairsFile with benchmark..."
        	pairsFileBaseName=$( basename $pairsFile )

        # Sort the pairs file.
        #sort -t, -k1d,1 -k4n,4 -k2,2 -k3n,3 $pairsFile > $directory/sorted_$pairsFileBaseName
                (head -n 1 $pairsFile && tail -n +2 $pairsFile | sort -t, -k2d,2 -k1d,1 -k3,3 -k4,4 -k7n,7 -k8n,8 -k5,5 -k6,6 -k9,9 -k10n,10 ) > $directory/sorted_$pairsFileBaseName
        	sortResult=$?
        	if [ ${sortResult} -ne 0 ] # Sorting failed.
        	then
            	testResult=$(( ${testResult} + ${SORT_FAILED_RESULT} ))
        	fi

        # Check for benchmark and, if there, diff.
        	if [ -f ${benchmarkDir}/sorted_$pairsFileBaseName \
             	-a -f $directory/sorted_$pairsFileBaseName ]
        	then
            # both files exist, do the comparison
            # Avoid pipe to "tee" because it hides the exit code
                diff -w --brief $directory/sorted_$pairsFileBaseName ${benchmarkDir}/sorted_$pairsFileBaseName
            	pairsDiffResult=$?

            	if [ ${pairsDiffResult} -ne 0 ]
            	then
                	echo "$echoPrefix The pairs file $directory/sorted_$pairsFileBaseName does not match the benchmark." | tee /dev/stderr
                	testResult=$(( ${testResult} + ${DIFFERENT_PAIRS_RESULT} ))
            	fi
        	elif [ ! -f $directory/$pairsFileBaseName ]
        	then
            	echo "$echoPrefix Not comparing pairs file with benchmark: File $directory/$pairsFileBaseName not found."
        	elif [ ! -f ${benchmarkDir}/sorted_$pairsFileBaseName ]
        	then
            	echo "$echoPrefix Not comparing pairs File with benchmark: ${benchmarkDir}/sorted_$pairsFileBaseName not found."
        	fi
    	done
	fi
done

# ==============================================
# Check metric output .csv files
# ==============================================

# Comparing metric otuput .csv files that exist in both ${benchmarkDir} and outputs.
echo "$echoPrefix Comparing output .csv files with ${benchmarkDir} if the benchmark version exists..."
csvDiffFlag=false

for csvFile in $(ls ${outputDirPrefix}* | grep csv | grep -v pairs)
do
    if [ -f ${outputDirPrefix}*/$csvFile -a -f ${benchmarkDir}/$csvFile ]
    then
        # Avoid pipe to "tee" because it hides the exit code
        diff -w -q ${outputDirPrefix}*/$csvFile ${benchmarkDir}/$csvFile
        csvDiffResult=$?

        if [ ${csvDiffResult} -ne 0 \
            -a "${csvDiffFlag}" == "false" ]
        then
            testResult=$(( ${testResult} + ${DIFFERENT_CSVS_RESULT} ))
            csvDiffFlag="true"
        fi
    fi
done
if [ "${csvDiffFlag}" == "true" ]
then
    echo "$echoPrefix At least one metric output .csv file was different than the benchmark." | tee /dev/stderr
fi

# Return the overall diff result, which is a sum of all of the accumulated differences, noting that the
# values summed are factors of two, so that you can identify what differences were found based on the number.
exit $testResult
