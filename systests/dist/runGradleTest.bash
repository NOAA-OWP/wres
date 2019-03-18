#!/bin/bash

# Run the scenario tests by used gradle
# must provide the 1st argument for the built version number as yyyymmdd-hhhhhh, where hhhhhh is hexadecimal 
#
# the 2nd argument is the test series, it can be either 0 (default), 700, or 900
# 0 is test series from 000, to 600, and 800
# 700 is test series for 700 only
# 900 is test series for 900 only
#
# Author: Raymond.Chui@***REMOVED***
# Created: March, 2019

if [ $# -lt 1 ]
then
	echo "Usage: $0 yyyymmdd-hhhhh [-r 0|700|900] [-d Scenario###]"
	exit
fi
built_number=$1
shift
#echo $built_number
series=0 # default
debug="NO"
while getopts "r:d:" opt; do
	case $opt in
		r)
			series=$OPTARG
			;;
		d)
			debug=$OPTARG
			;;
		\?)
			echo "Usage: $0 yyyymmdd-hhhhh [-r 0|700|900] [-d Scenario###]"
			exit 2
			;;
	esac
done
echo "built_number = $built_number, series = $series, debug = $debug"
#exit

if [ ! -f /wres_share/releases/archive/wres-"$built_number".zip ]
then
	echo "/wres_share/releases/archive/wres-"$built_number".zip doesn't exist."
	exit
fi

cd /home/raymond.chui/wres_testing/wres/systests/dist

# if the test built hasn't unziped yet, then remove the all old builts
if [ ! -d build/wres-"$built_number" ]
then
	oldBuilts=`ls -d build/wres-*`
	echo $oldBuilts
	if [ -n $oldBuilt ]
	then
		for oldBuilt in $oldBuilts 
		do
			if [ -d $oldBuilt ]
			then
				rm -rf $oldBuilt
				echo "Removed old built $oldBuilt"
			fi
		done
	fi
elif [ -d build/wres-"$built_number" ]
then
	echo -n "Test the "
	ls -d build/wres-"$built_number"
else
	echo -n "Copy the revision to build/wres-"
	echo $built_number
fi

echo "Ready to test /wres_share/releases/archive/wres-"$built_number".zip"

if [ "$debug" != "NO" ]
then
	echo "Do debug for $debug"
	../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=***REMOVED***wresdb-dev01.***REMOVED***.***REMOVED*** -Dwres.username=wres_user7 -Dwres.databaseName=wres7 -Djava.awt.headless=true" --tests=$debug | tee debug.txt 
elif [ "$debug" == "NO" ]
then
	if [ $series -eq 0 ]
	then
		echo "test --tests=Scenario0* --tests=Scenario1* --tests=Scenario2* --tests=Scenario3* --tests=Scenario4* --tests=Scenario5* --tests=Scenario6* --tests=Scenario8*"

		../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=***REMOVED***wresdb-dev01.***REMOVED***.***REMOVED*** -Dwres.username=wres_user7 -Dwres.databaseName=wres7 -Djava.awt.headless=true" --tests=Scenario0* --tests=Scenario1* --tests=Scenario2* --tests=Scenario3* --tests=Scenario4* --tests=Scenario5* --tests=Scenario6* --tests=Scenario8* | tee testOutputs.txt 2>&1
		#	../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=***REMOVED***wresdb-dev01.***REMOVED***.***REMOVED*** -Dwres.username=wres_user7 -Dwres.databaseName=wres7 -Djava.awt.headless=true" --tests=Scenario5* | tee testOutputs.txt 2>&1
		#	../../gradlew cleanTest test --debug -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=***REMOVED***wresdb-dev01.***REMOVED***.***REMOVED*** -Dwres.username=wres_user7 -Dwres.databaseName=wres7 -Djava.awt.headless=true" --tests=Scenario501 | tee testOutputs.txt
	elif [ $series -eq 700 ]
	then
		echo "test --tests=Scenario7*"
		../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=***REMOVED***wresdb-dev01.***REMOVED***.***REMOVED*** -Dwres.username=wres_user7 -Dwres.databaseName=wres7 -Djava.awt.headless=true" --tests=Scenario7* | tee testOutputs700.txt
	elif [ $series -eq 900 ]
	then
		echo "test --tests=Scenario9*"
		../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=***REMOVED***wresdb-dev01.***REMOVED***.***REMOVED*** -Dwres.username=wres_user7 -Dwres.databaseName=wres7 -Djava.awt.headless=true" --tests=Scenario9* | tee testOutputs900.txt
	else
		echo "Unknown test series $series"
	fi
fi
