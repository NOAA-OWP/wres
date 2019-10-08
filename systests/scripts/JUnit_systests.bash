#!/bin/bash

# Run the system tests with JUnit
# Author: Raymond.Chui@***REMOVED***
# Created: October, 2019

# source the WRES environment variables
#. WRES_Variables.txt 
. ~/.bash_profile

TIMESTAMP=$(/usr/bin/date +"%Y%m%d%H%M%S")
LOGFILE=JUnit_systestsLog_${TIMESTAMP}.txt
/usr/bin/touch $LOGFILE

if [ -z "$WRES_DB_NAME" ]
then
	echo "Please export WRES_DB_NAME in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
	exit 2 
else
	echo "WRES_DB_NAME = $WRES_DB_NAME"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_LOG_LEVEL" ]
then
	export WRES_LOG_LEVEL=info
fi
echo "WRES_LOG_LEVEL = $WRES_LOG_LEVEL"  2>&1 | /usr/bin/tee --append $LOGFILE
#echo "TESTS_DIR = $TESTS_DIR"
if [ -z "$WRES_DB_USERNAME" ]
then
	echo "Please export WRES_DB_USERNAME in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
	exit 2
else
	echo "WRES_DB_USERNAME = $WRES_DB_USERNAME"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_DB_HOSTNAME" ]
then
	echo "Please export WRES_DB_HOSTNAME in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
	exit 2
else
	echo "WRES_DB_HOSTNAME = $WRES_DB_HOSTNAME"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ ! -s ~/.pgpass ]
then
	echo "Please enter WRES_DB_HOSTNAME:PORT:WRES_DB_NAME:WRES_DB_USER:{password} in ~/.pgpass"  2>&1 | /usr/bin/tee --append $LOGFILE
	exit 2
else
	ls -l ~/.pgpass 2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_DIR" ]
then
	echo "Please export WRES_DIR in ~/.bash_profile" 2>&1 | /usr/bin/tee --append $LOGFILE
	exit 2
else
	cd $WRES_DIR
	/usr/bin/pwd 2>&1 | /usr/bin/tee --append $LOGFILE	
fi
#echo "WRES_DB_PASSWORD = $WRES_DB_PASSWORD"
#echo "TIMESTAMP = $TIMESTAMP"
#$echo "LOGFILE = $LOGFILE"

./gradlew distZip 2>&1 | /usr/bin/tee --append $LOGFILE 
REVISION=`/usr/bin/find build/distributions | /usr/bin/tail -1 | /usr/bin/gawk -F / '{print($NF)}'  | /usr/bin/sed -e s/wres-// | /usr/bin/tr -d '.zip'`
# where tail -1 is get the latest built from the find command
# gawk will make sure get the last field of build/distributions/wres-revision.zip
# and sed and tr will cut out the head 'wres-' and tail '.zip'
echo "Revision = $REVISION" 2>&1 | /usr/bin/tee --append $LOGFILE

cd systests
../gradlew installDist 2>&1 | /usr/bin/tee --append $LOGFILE
cd build/install/systests
/usr/bin/mkdir -pv outputs 2>&1 | /usr/bin/tee --append $LOGFILE
/usr/bin/pwd 2>&1 | /usr/bin/tee --append $LOGFILE

./gradlew cleanTest test -PversionToTest=$REVISION -PwresZipDirectory=../../../../build/distributions -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.username=$WRES_DB_USERNAME -Dwres.url=$WRES_DB_HOSTNAME -Dwres.databaseName=$WRES_DB_NAME -Dwres.dataDirectory=../../.. -Djava.io.tmpdir=./outputs" --tests="SystemTestSuite" -$WRES_LOG_LEVEL 2>&1 | /usr/bin/tee --append $LOGFILE
#./gradlew cleanTest test -PversionToTest=$REVISION -PwresZipDirectory=../../../../build/distributions -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.username=$WRES_DB_USERNAME -Dwres.url=$WRES_DB_HOSTNAME -Dwres.databaseName=$WRES_DB_NAME -Dwres.dataDirectory=../../.. -Dwres.password=$WRES_DB_PASSWORD -Djava.io.tmpdir=./outputs" --tests="SystemTestSuite" -$WRES_LOG_LEVEL 2>&1 | /usr/bin/tee --append $LOGFILE

/usr/bin/grep FAILED $LOGFILE | /usr/bin/grep testScenario > failures.txt 
/usr/bin/grep PASSED $LOGFILE | /usr/bin/grep testScenario > passes.txt 
if [ -s failures.txt ]
then
	failure_nums=`/usr/bin/cat failures.txt | /usr/bin/wc -l`
	/usr/bin/mailx -F -S smtp=140.90.91.135 -s "JUnit Test $REVISION $failure_nums FAILED" -a $LOGFILE Raymond.Chui@***REMOVED*** < failures.txt
	rm -v failures.txt passes.txt
elif [ -s passes.txt ]
then
	pass_nums=`/usr/bin/cat passes.txt | /usr/bin/wc -l`
	/usr/bin/mailx -F -S smtp=140.90.91.135 -s "JUnit Test $REVISION $pass_nums PASSED" -a $LOGFILE Raymond.Chui@***REMOVED*** < passes.txt
	rm -v passes.txt failures.txt
fi
