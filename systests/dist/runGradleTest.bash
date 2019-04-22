#!/bin/bash

# Run the scenario tests by used gradle
# must provide the 1st argument for the built version number as yyyymmdd-hhhhhh, where hhhhhh is hexadecimal 
#
# the -r option argument is the test series, it can be either 0 (default), 700, or 900
# 0 is test series from 000, to 600, and 800
# 700 is test series for 700 only
# 900 is test series for 900 only
# -d is debuggig, argument can be Scenario### or Scenario#*
#
# Author: Raymond.Chui@***REMOVED***
# Created: March, 2019

if [ $# -lt 1 ]
then
	echo "Usage: $0 yyyymmdd-hhhhh [-r 0|1|700|900] [-d Scenario###] [-m yes|no]"
	exit
fi
built_number=$1
shift
#echo $built_number
series=0 # default
debug="NO"
multipleTest="Yes"
while getopts "r:d:m:" opt; do
	case $opt in
		r)
			series=$OPTARG
			;;
		d)
			debug=$OPTARG
			;;
		m)
			multipleTest=$OPTARG
			;;
		\?)
			echo "Usage: $0 yyyymmdd-hhhhh [-r 0|1|700|900] [-d Scenario###] [-m yes|no]"
			exit 2
			;;
	esac
done

multipleTest=`echo $multipleTest | tr [a-z] [A-Z]`
# assume you export your WRES environment variables in your ~/.bash_profile
. ~/.bash_profile
# If you stored in diffeernt place, then you need to export them into here.
# export WRES_DB_NAME=
# export WRES_LOG_LEVEL=debug
# export TESTS_DIR=~/wres_testing/wres/systests
# export WRES_LOG_LEVEL=info
# export WRES_DB_USERNAME=
# export WRES_DB_HOSTNAME=***REMOVED***wresdb-dev01.***REMOVED***.***REMOVED***

echo "built_number = $built_number, series = $series, debug = $debug"
#exit

if [ ! -f /wres_share/releases/archive/wres-"$built_number".zip ]
then
	echo "/wres_share/releases/archive/wres-"$built_number".zip doesn't exist."
	exit
fi

if [ -z "$TESTS_DIR" ]
then
	echo "Please 'export TESTS_DIR=~/your_paths/systests'"
    exit 2
elif [ ! -d $TESTS_DIR ]
then
	echo "$TESTS_DIR is not a directory or doesn't exist"
	ls $TESTS_DIR
	exit 2
fi
cd $TESTS_DIR
# -------------- Note these are UNIX/LINUX commands, Windows may not OK
mkdir -pv outputs
cd outputs
rm -rf *
# --------------------
cd $TESTS_DIR/dist
pwd
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
	if [  $series -eq 1 ]
	then
		echo "Do debug for $debug"
		../../gradlew cleanTest test --debug -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=$WRES_DB_HOSTNAME -Dwres.username=$WRES_DB_USERNAME -Dwres.databaseName=$WRES_DB_NAME -Djava.awt.headless=true" --tests=$debug | tee debug.txt 2>&1
	elif [ $series -eq 0 ]
	then
		../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=$WRES_DB_HOSTNAME -Dwres.username=$WRES_DB_USERNAME -Dwres.databaseName=$WRES_DB_NAME -Djava.awt.headless=true" --tests=$debug | tee testOutputs.txt 2>&1
	fi
elif [ "$debug" = "NO" ]
then
	if [ $series -eq 0 ]
	then
		if [ -f testOutputs.txt ]
		then
			rm -v testOutputs.txt
		fi
		# try to test by wild cards
#		tests="--tests=Scenario00* --tests=Scenario01*  --tests=Scenario05* --tests=Scenario1* --tests=Scenario2* --tests=Scenario3* --tests=Scenario4* --tests=Scenario5* --tests=Scenario6* --tests=Scenario8*"
#		echo "Test these classes: $tests" | tee testOutputs.txt
#		../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=$WRES_DB_HOSTNAME -Dwres.username=$WRES_DB_USERNAME -Dwres.databaseName=$WRES_DB_NAME -Djava.awt.headless=true" --tests=$tests | tee -a testOutputs.txt 2>&1 

		testClasses=`ls build/classes/java/test/wres/systests/Scenario[0-6,8]*.class | gawk -F/ '{print($NF)}' | cut -d'.' -f1`
		if [ "$multipleTest" = "NO" ]
		then
			# try to do one by one
				echo "Test these classes: $testClasses" | tee testOutputs.txt
				for testClass in $testClasses
        		do
					tests="--tests=$testClass"
					echo $tests | tee -a testOutputs.txt 2>&1
					../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=$WRES_DB_HOSTNAME -Dwres.username=$WRES_DB_USERNAME -Dwres.databaseName=$WRES_DB_NAME -Djava.awt.headless=true" $tests | tee -a testOutputs.txt 2>&1 
				done
		elif [ "$multipleTest" = "YES" ]
		then
			# Do multiple tests
			tests=
			for testClass in $testClasses
			do
				tests="$tests --tests=$testClass "
			done
#		#echo $tests
		#echo "$tests" | tee -a testOutputs.txt
			../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=$WRES_DB_HOSTNAME -Dwres.username=$WRES_DB_USERNAME -Dwres.databaseName=$WRES_DB_NAME -Djava.awt.headless=true" $tests | tee -a testOutputs.txt 2>&1 
		fi
	elif [ $series -eq 700 ]
	then
		echo "test --tests=Scenario7*"
		../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=$WRES_DB_HOSTNAME -Dwres.username=$WRES_DB_USERNAME -Dwres.databaseName=$WRES_DB_NAME -Djava.awt.headless=true" --tests=Scenario7* | tee testOutputs700.txt 2>&1
	elif [ $series -eq 900 ]
	then
		echo "test --tests=Scenario9*"
		../../gradlew cleanTest test -PwresZipDirectory=/wres_share/releases/archive -PversionToTest=$built_number -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.url=$WRES_DB_HOSTNAME -Dwres.username=$WRES_DB_USERNAME -Dwres.databaseName=$WRES_DB_NAME -Djava.awt.headless=true" --tests=Scenario9* | tee testOutputs900.txt 2>&1
	else
		echo "Unknown test series $series"
	fi
fi
