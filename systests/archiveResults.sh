#!/bin/bash

# archive the test results
# Author: Raymond.Chui@***REMOVED***
# Created: December, 2018

LATEST=$1
dirs=`ls -d scenario0* scenario1* scenario2* scenario3* scenario4* scenario5* scenario6* scenario8* scenario7*`

mkdir -pv SystemTestsOutputs
# rm -v SystemTestsOutputs/*

tar -czvf SystemTestsOutputs/systests.$LATEST.tar.gz $dirs


#for dir in $dirs
#do
#	#echo $dir
#	cd $dir
#	#pwd
#	subdir=`ls -d wres_evaluation_output* 2> /dev/null`
#	#echo $subdir
#	if [ -n "$subdir" ]
#	then
#		if [ -d $subdir ]
#		then
#			#echo "$dir"_"$subdir".tar.gz
#			tar -czvf "$dir"_"$subdir".tar.gz $subdir/
#			mv -v "$dir"_"$subdir".tar.gz ../SystemTestsOutputs
#			rm -rf $subdir/
#		fi
#	fi
#	cd ..
#done
