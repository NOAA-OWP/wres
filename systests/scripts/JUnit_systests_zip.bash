#!/bin/bash

# Run the system tests with JUnit
# Author: Raymond.Chui@***REMOVED***
# Created: October, 2019

# source the WRES environment variables
#. WRES_Variables.txt 
. ~/.bash_profile
cd ~
TIMESTAMP=$(/usr/bin/date +"%Y%m%d%H%M%S")
TOPPWD=/wres_share/releases/JUnitTests
LOGFILE=$TOPPWD/JUnit_systestsLog_${TIMESTAMP}.txt
TESTINGJ=/wres_share/releases/install_scripts/testingJ.txt
PENDINGQUEUEJ=/wres_share/releases/pendingQueueJ.txt
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
#/usr/bin/touch $LOGFILE
if [ -f /wres_share/releases/systests/installing ]
then
	ls -l /wres_share/releases/systests/installing  2>&1 | /usr/bin/tee --append $LOGFILE
	echo "The system is installing the new built, now" 2>&1 | /usr/bin/tee --append $LOGFILE
	exit
fi
if [ -f $TESTINGJ ]
then
	ls -l $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE
	echo "There is a system test running, now" 2>&1 | /usr/bin/tee --append $LOGFILE
	exit
fi
touch $TESTINGJ 

REVISION=
WRES_REVISION=
if [ ! -s $PENDINGQUEUEJ ]
then
	# there isn't any queue
	ls -l $PENDINGQUEUEJ 2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	exit
elif [ -s $PENDINGQUEUEJ ]
then
	REVISION=`head -1 $PENDINGQUEUEJ | /usr/bin/sed -e s/wres-//` 
	WRES_REVISION=`head -1 $PENDINGQUEUEJ` 
fi

echo "################ $TIMESTAMP #######################" 2>&1 | /usr/bin/tee --append $LOGFILE

if [ -z "$WRES_DB_NAMEJ" ]
then
	echo "Please export WRES_DB_NAMEJ in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	exit 2 
else
	echo "WRES_DB_NAMEJ = $WRES_DB_NAMEJ"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_LOG_LEVELJ" ]
then
	export WRES_LOG_LEVELJ=info
fi
echo "WRES_LOG_LEVELJ = $WRES_LOG_LEVELJ"  2>&1 | /usr/bin/tee --append $LOGFILE
#echo "TESTS_DIR = $TESTS_DIR"
if [ -z "$WRES_DB_USERNAMEJ" ]
then
	echo "Please export WRES_DB_USERNAMEJ in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	exit 2
else
	echo "WRES_DB_USERNAMEJ = $WRES_DB_USERNAMEJ"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_DB_HOSTNAMEJ" ]
then
	echo "Please export WRES_DB_HOSTNAMEJ in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	exit 2
else
	echo "WRES_DB_HOSTNAMEJ = $WRES_DB_HOSTNAMEJ"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ ! -s ~/.pgpass ]
then
	echo "Please enter WRES_DB_HOSTNAMEJ:PORT:WRES_DB_NAMEJ:WRES_DB_USERJ:{password} in ~/.pgpass"  2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	exit 2
else
	ls -l ~/.pgpass 2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_DIRJ" ]
then
	echo "Please export WRES_DIRJ in ~/.bash_profile" 2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	exit 2
else
	cd $WRES_DIRJ
	echo -n "WRES_DIRJ: " 2>&1 | /usr/bin/tee --append $LOGFILE
	/usr/bin/pwd 2>&1 | /usr/bin/tee --append $LOGFILE	
fi
#echo "WRES_DB_PASSWORD = $WRES_DB_PASSWORD"
#echo "TIMESTAMP = $TIMESTAMP"
#$echo "LOGFILE = $LOGFILE"


# Since we archived the latest zip file, we could specify the wresZipDirectory and the latest revision
wresZipDirectory=/wres_share/releases/archive
if [ ! -s $wresZipDirectory/wres-${REVISION}.zip ]
then
	ls -l $wresZipDirectory/wres-${REVISION}.zip 2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	exit
else
	echo "Revision = $REVISION" 2>&1 | /usr/bin/tee --append $LOGFILE
	ls -l $wresZipDirectory/wres-${REVISION}.zip 2>&1 | /usr/bin/tee --append $LOGFILE
	cd $WRES_DIRJ/systests
	echo -n "systests dir: " 2>&1 | /usr/bin/tee --append $LOGFILE
	/usr/bin/pwd 2>&1 | /usr/bin/tee --append $LOGFILE
	SYSTESTS_DIR=`/usr/bin/pwd | gawk -F/ '{print($NF)}'` 
	
#	if [ ! -d  build/install/systests ]
#	then
#		mkdir -pv  build/install/systests
#		./gradlew distZip -PversionToTest=$REVISION -PwresZipDirectory=$wresZipDirectory --info 2>&1 | /usr/bin/tee --append $LOGFILE
#	fi

#	echo "..........   ./gradlew installDist -PversionToTest=$REVISION -PwresZipDirectory=$wresZipDirectory --debug" # 2>&1 | /usr/bin/tee --append $LOGFILE
#	./gradlew installDist -PversionToTest=$REVISION -PwresZipDirectory=$wresZipDirectory --info 2>&1 | /usr/bin/tee --append $LOGFILE

#	if [ -d build/install/systests ]
#	then
#		cd build/install/systests
#		/usr/bin/mkdir -pv outputs 2>&1 | /usr/bin/tee --append $LOGFILE
#		/usr/bin/pwd 2>&1 | /usr/bin/tee --append $LOGFILE
#	else
#		ls -d build/install/systests 2>&1 | /usr/bin/tee --append $LOGFILE
#		echo "Failed to build installDist" 2>&1 | /usr/bin/tee --append $LOGFILE
#		rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE
#		exit
#	fi

	mkdir -pv outputs

	echo "	./gradlew cleanTest test -PversionToTest=$REVISION -PwresZipDirectory=$wresZipDirectory -PtestJvmSystemProperties=\"-Dwres.useSSL=true -Dwres.username=$WRES_DB_USERNAMEJ -Dwres.url=$WRES_DB_HOSTNAMEJ -Dwres.databaseName=$WRES_DB_NAMEJ -Dwres.dataDirectory=/wres_share/testing -Dwres.logLevel=$WRES_LOG_LEVELJ -Djava.io.tmpdir=./outputs\" --tests=\"SystemTestSuite\" --$WRES_LOG_LEVELJ"
	./gradlew cleanTest test -PversionToTest=$REVISION -PwresZipDirectory=$wresZipDirectory -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.username=$WRES_DB_USERNAMEJ -Dwres.url=$WRES_DB_HOSTNAMEJ -Dwres.databaseName=$WRES_DB_NAMEJ -Dwres.dataDirectory=. -Djava.io.tmpdir=./outputs" --tests="SystemTestSuite" --$WRES_LOG_LEVELJ 2>&1 | /usr/bin/tee --append $LOGFILE
fi

#/usr/bin/grep FAILED $LOGFILE | /usr/bin/grep testScenario > failures.txt 
/usr/bin/grep failed build/reports/tests/test/classes/*.html > failures.txt 
#/usr/bin/grep PASSED $LOGFILE | /usr/bin/grep testScenario > passes.txt 
/usr/bin/grep passed build/reports/tests/test/classes/*.html > passes.txt 
if [ -s failures.txt ]
then
	failure_nums=`/usr/bin/cat failures.txt | /usr/bin/wc -l`
else
	failure_nums=0
fi
if [ -s passes.txt ]
then
	pass_nums=`/usr/bin/cat passes.txt | /usr/bin/wc -l`
else
	pass_nums=0
fi
cat passes.txt failures.txt > summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE
MAIL_SUBJECT="JUnit in $SYSTESTS_DIR Tested $WRES_REVISION : $pass_nums PASSED; $failure_nums FAILED"
/usr/bin/mailx -F -S smtp=140.90.91.135 -s "$MAIL_SUBJECT" -a $LOGFILE Raymond.Chui@***REMOVED***,Hank.Herr@***REMOVED***,james.d.brown@***REMOVED***,jesse.bickel@***REMOVED***,christopher.tubbs@***REMOVED*** < summary.txt  2>&1 | /usr/bin/tee --append $LOGFILE
rm -v passes.txt failures.txt summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE

# remove JUnit test lock file
rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
# delete the top (1st) line of JUnit pending queue
echo "Remove $REVISION from $PENDINGQUEUEJ" 2>&1 | /usr/bin/tee --append $LOGFILE
/usr/bin/sed  -i '1d' $PENDINGQUEUEJ 2>&1 | /usr/bin/tee --append $LOGFILE

# remove the log files older than 24 hours
echo "/usr/bin/find -P $TOPPWD -maxdepth 1 -name \"JUnit_systestsLog_*\" -mtime +1 -exec rm -v {} \;" # 2>&1 | /usr/bin/tee --append $LOGFILE
/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE

if [ -d outputs ]
then
	cd outputs
	/usr/bin/pwd 2>&1 | /usr/bin/tee --append $LOGFILE	
	# remove outout archive older than 1 days.
	find -P . -maxdepth 1 -name "wres_evaluation_output_*" -mtime +1 -exec rm -rf {} \; 2>&1 | /usr/bin/tee --append $LOGFILE
else
	ls -d outputs 2>&1 | /usr/bin/tee --append $LOGFILE
fi
