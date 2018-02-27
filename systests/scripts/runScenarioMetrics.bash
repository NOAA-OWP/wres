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
if [ ! -d $systests_dir ]
then
	echo "$systests_dir directory is not existed"
	exit 2
fi
cd $systests_dir

if [ $# -ge 2 ]
then
	scenario_dirs=$2
else # run all scenarios
	scenario_dirs=$(ls -d scenario*)
fi

#MetricsScriptDir=/wres_share/releases/systests
MetricsScriptDir=../..

for scenario_dir in $scenario_dirs
do
	if [ -d $scenario_dir/output ]
	then
		cd $scenario_dir/output
		$MetricsScriptDir/scripts/checkSorted.bash sorted_pairs.csv > checkedSorted_pairs.csv
		#touch error.txt
		pwd
		if [ -f testMetricsResults.txt ]
		then # remove the old results file
			rm -v testMetricsResults.txt
		fi
		#$MetricsScriptDir/scripts/createMetricsTest.bash
		$MetricsScriptDir/scripts/createMetricsTest.bash
		rm -v temp1.txt header.txt metricsValues.txt fileValues.txt joinFiles.txt
		#if [ ! -s error.txt ]
		#then # remove it if is an empty file (no error occured)
		#	rm -v error.txt
		#fi
		cd ../..
	fi
done
