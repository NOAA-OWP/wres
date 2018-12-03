#!/bin/bash

#dirs=`ls -d scenario0*/wres_evaluation_output*`
#dirs=`ls -d scenario0*/wres_evaluation_output* scenario1*/wres_evaluation_output* scenario2*/wres_evaluation_output* scenario3*/wres_evaluation_output* scenario4*/wres_evaluation_output* scenario5*/wres_evaluation_output* scenario6*/wres_evaluation_output* scenario8*/wres_evaluation_output*`
dirs=`ls -d scenario0* scenario1* scenario2* scenario3* scenario4* scenario5* scenario6* scenario8* scenario7*`

mkdir -pv SystemTestsOutputs
rm -v SystemTestsOutputs/*
for dir in $dirs
do
	#echo $dir
	cd $dir
	#pwd
	subdir=`ls -d wres_evaluation_output* 2> /dev/null`
	#echo $subdir
	if [ -n "$subdir" ]
	then
		if [ -d $subdir ]
		then
			#echo "$dir"_"$subdir".tar.gz
			tar -czvf "$dir"_"$subdir".tar.gz $subdir/
			mv -v "$dir"_"$subdir".tar.gz ../SystemTestsOutputs
		fi
	fi
	cd ..
done
