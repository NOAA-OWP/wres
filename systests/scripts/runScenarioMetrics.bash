#!/bin/bash

# Run the Metrics test for the scenarios
# you must provide the systests directory.
# if there are no scenario directories provide, then will run all.
# Author: Raymond.Chui@***REMOVED***
# Created: Feb., 2018

if [ $# -lt 1 ]
then
	echo "Usage $0 systests_dir [scenario_dirs]"
	exit 2
fi
systests_dir=$1
#scenario_dirs=$2
echo "$systests_dir $scenario_dirs"

if [ ! -d $systests_dir ]
then
	echo "$systests_dir directory is not existed"
	exit 2
fi
cd $systests_dir
pwd
#scenario_dirs=`ls -d $2`
if [ $# -ge 2 ]
then
#	echo $2
	scenario_dirs=$2
else # run all scenarios
	scenario_dirs=$(ls -d scenario*)
fi

echo $scenario_dirs

#MetricsScriptDir=/wres_share/releases/systests
MetricsScriptDir=../..

for scenario_dir in $scenario_dirs
do
	if [ -d $scenario_dir/output ]
	then
		cd $scenario_dir/output
		if [ -f sorted_pairs.csv -a -f dirListing.txt ]
		then
			#is400=`echo $scenario_dir | egrep '(scenario4|scenario5)'`
			is400=`echo $scenario_dir | egrep '(scenario4)'`
			if [ -n "$is400" ]
			then
				$MetricsScriptDir/scripts/prepaireFiles.bash
			fi
			#if [ -f checkedSorted_pairs.csv ]
			#then
			#	rm -v checkedSorted_pairs.csv
			#fi
			#is400=`echo $scenario_dir | grep scenario4`
			#if [ -n "$is400" ]
			#then
			#	($MetricsScriptDir/scripts/checkSorted.bash sorted_pairs.csv > checkedSorted_pairs.csv) 2> error.txt
			#	theDiff=`diff -q sorted_pairs.csv checkedSorted_pairs.csv`
			#	if [ -z "$theDiff" ]
			#	then
			#		echo "There are no extra column in sorted_pairs.csv"
			#		rm -v checkedSorted_pairs.csv
			#	fi
			#fi
			pwd
			#if [ -f testMetricsResults.txt ]
			#then # remove the old results file
			#	rm -v testMetricsResults.txt
			#fi
			if [ -f IDFile.txt ]
			then
				for ID in `cat IDFile.txt`
				do
					$MetricsScriptDir/scripts/createMetricsTest.bash $ID
				done
			else
				$MetricsScriptDir/scripts/createMetricsTest.bash
			fi
			rm -v temp1.txt header.txt metricsValues.txt fileValues.txt joinFiles.txt
			if [ -f error.txt -a ! -s error.txt ]
			then # remove it if is an empty file (no error occured)
				rm -v error.txt
			fi
		fi
		cd ../..
	fi
done
