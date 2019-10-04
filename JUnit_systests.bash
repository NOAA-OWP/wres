#!/bin/bash

# Run the system tests with JUnit
# Author: Raymond.Chui@***REMOVED***
# Created: October, 2019

# source the WRES environment variables
. WRES_Variables.txt 

TIMESTAMP=$(/usr/bin/date +"%Y%m%d%H%M%S")
echo "WRES_DB_NAME = $WRES_DB_NAME"
echo "WRES_LOG_LEVEL = $WRES_LOG_LEVEL"
echo "TESTS_DIR = $TESTS_DIR"
echo "WRES_DB_USERNAME = $WRES_DB_USERNAME"
echo "WRES_DB_HOSTNAME = $WRES_DB_HOSTNAME"
echo "WRES_DB_PASSWORD = $WRES_DB_PASSWORD"
#echo "TIMESTAMP = $TIMESTAMP"
LOGFILE=JUnit_systestsLog_${TIMESTAMP}.txt
echo "LOGFILE = $LOGFILE"
/usr/bin/touch $LOGFILE

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

./gradlew cleanTest test -PversionToTest=$REVISION -PwresZipDirectory=../../../../build/distributions -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.username=$WRES_DB_USERNAME -Dwres.url=$WRES_DB_HOSTNAME -Dwres.databaseName=$WRES_DB_NAME -Dwres.dataDirectory=../../.. -Dwres.password=$WRES_DB_PASSWORD -Djava.io.tmpdir=./outputs" --tests="SystemTestSuite" -$WRES_LOG_LEVEL 2>&1 | /usr/bin/tee --append $LOGFILE
