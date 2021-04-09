#!/bin/bash

# Run the system tests with JUnit
# Author: Raymond.Chui@***REMOVED***
# Created: October, 2019

# source the WRES environment variables
#. WRES_Variables.txt 
. ~/.bash_profile
. ~/.mailrc
cd ~
TIMESTAMP=$(/usr/bin/date +"%Y%m%d%H%M%S")
TOPPWD=/wres_share/releases/JUnitTests
LOGFILE=$TOPPWD/JUnit_systestsLog_${TIMESTAMP}.txt
TESTINGJ=/wres_share/releases/install_scripts/testingJ.txt
PENDINGQUEUEJ=/wres_share/releases/pendingQueueJ.txt
wresGraphicsZipDirectory=/wres_share/releases/archive/graphics
#WRES_GROUP=Raymond.Chui@***REMOVED***,Hank.Herr@***REMOVED***,james.d.brown@***REMOVED***,jesse.bickel@***REMOVED***,christopher.tubbs@***REMOVED***,travis.quarterman@***REMOVED***,arthur.raney@***REMOVED***
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
#/usr/bin/touch $LOGFILE
if [ -f /wres_share/releases/systests/installing ]
then
	ls -l /wres_share/releases/systests/installing  2>&1 | /usr/bin/tee --append $LOGFILE
	echo "The system is installing the new built, now" 2>&1 | /usr/bin/tee --append $LOGFILE
	/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
	exit
fi
if [ -f $TESTINGJ ]
then
	ls -l $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE
	fileStatus=`/bin/stat $TESTINGJ | grep Change | cut -d'.' -f1 | gawk '{print($2,$3)}'`
	lastHours=`/wres_share/releases/install_scripts/testDateTime.py "$fileStatus"`
	echo "$TESTINGJ has created/changed at $lastHours ago at $fileStatus" 2>&1 | /usr/bin/tee --append $LOGFILE
	if [ $lastHours -gt 0 ]
	then	# Since that testingJ.txt lock file last for more than a hour, remove it!
		echo -n "System up time " 2>&1 | /usr/bin/tee --append $LOGFILE
		/bin/uptime -s | /usr/bin/tee --append $LOGFILE
		rm -v $TESTINGJ | /usr/bin/tee --append $LOGFILE
	else
		echo "There is a system test running, now" 2>&1 | /usr/bin/tee --append $LOGFILE
		/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
		exit
	fi
fi
touch $TESTINGJ 

REVISION=
WRES_REVISION=
if [ ! -s $PENDINGQUEUEJ ]
then
	# there isn't any queue
	ls -l $PENDINGQUEUEJ 2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
	exit
elif [ -s $PENDINGQUEUEJ ]
then
	REVISION=`head -1 $PENDINGQUEUEJ | /usr/bin/sed -e s/wres-//` 
	WRES_REVISION=`head -1 $PENDINGQUEUEJ` 
	echo "WRES_REVISION = $WRES_REVISION "  2>&1 | /usr/bin/tee --append $LOGFILE
	if [ -z $WRES_REVISION ]
	then # for some reasons, the 1st line is blank
		cat -n $PENDINGQUEUEJ  2>&1 | /usr/bin/tee --append $LOGFILE
		echo "Remove that blank line at top" 2>&1 | /usr/bin/tee --append $LOGFILE
		/usr/bin/sed  -i '1d' $PENDINGQUEUEJ 2>&1 | /usr/bin/tee --append $LOGFILE
		rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE
        	/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
		exit
	else
		echo "WRES_REVISION = $WRES_REVISION; REVISION = $REVISION" 2>&1 | /usr/bin/tee --append $LOGFILE
	fi
fi

echo "################ $TIMESTAMP #######################" 2>&1 | /usr/bin/tee --append $LOGFILE

if [ -z "$WRES_DB_NAMEJ" ]
then
	echo "Please export WRES_DB_NAMEJ in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
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
	/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
	exit 2
else
	echo "WRES_DB_USERNAMEJ = $WRES_DB_USERNAMEJ"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_DB_HOSTNAMEJ" ]
then
	echo "Please export WRES_DB_HOSTNAMEJ in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
	exit 2
else
	echo "WRES_DB_HOSTNAMEJ = $WRES_DB_HOSTNAMEJ"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ ! -s ~/.pgpass ]
then
	echo "Please enter WRES_DB_HOSTNAMEJ:PORT:WRES_DB_NAMEJ:WRES_DB_USERJ:{password} in ~/.pgpass"  2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
	exit 2
else
	ls -l ~/.pgpass 2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_DIRJ" ]
then
	echo "Please export WRES_DIRJ in ~/.bash_profile" 2>&1 | /usr/bin/tee --append $LOGFILE
	rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE 
	/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
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
	/usr/bin/find -P $TOPPWD -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +1 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
	exit
else
	echo "Revision = $REVISION" 2>&1 | /usr/bin/tee --append $LOGFILE
	ls -l $wresZipDirectory/wres-${REVISION}.zip 2>&1 | /usr/bin/tee --append $LOGFILE
	cd $WRES_DIRJ/systests
	echo -n "systests dir: " 2>&1 | /usr/bin/tee --append $LOGFILE
	/usr/bin/pwd 2>&1 | /usr/bin/tee --append $LOGFILE
	SYSTESTS_DIR=`/usr/bin/pwd | gawk -F/ '{print($NF)}'` 
	if [ ! -L data ]
	then	# if the data link doesn't exist, then create it
		ls -l data 2>&1 | /usr/bin/tee --append $LOGFILE
		ln -sv /wres_share/testing/data data 2>&1 | /usr/bin/tee --append $LOGFILE
	fi
	
#	On 2020-02-13 a day before the Valentine's Day we got
#	java.io.IOException: The given database URL ('***REMOVED***wresdb-dev01.***REMOVED***.***REMOVED***') is not accessible. error
#	So, we need to test the DB connection before run scenario tests

	/bin/psql --host $WRES_DB_HOSTNAMEJ --dbname $WRES_DB_NAMEJ --username $WRES_DB_USERNAMEJ --list --output testDBConnection.txt 2> testDBError.txt
	ls -l testDBConnection.txt testDBError.txt 2>&1 | /usr/bin/tee --append $LOGFILE
	if [ -s testDBError.txt ]
	then
		MAIL_SUBJECT="Dudes, JUnit in $SYSTESTS_DIR Tested $WRES_REVISION with Database problem"
#		/usr/bin/mailx -F -S smtp=nwcss-mail01.owp.nws.***REMOVED*** -s "$MAIL_SUBJECT" $WRES_GROUP < testDBError.txt  2>&1 | /usr/bin/tee --append $LOGFILE
		/usr/bin/mailx -F -A WRES_Setting -s "$MAIL_SUBJECT" -v WRES_GROUP < testDBError.txt  2>&1 | /usr/bin/tee --append $LOGFILE
		cat testDBError.txt 2>&1 | /usr/bin/tee --append $LOGFILE
		rm -v testDBError.txt testDBError.txt 2>&1 | /usr/bin/tee --append $LOGFILE
		rm -v $TESTINGJ 2>&1 | /usr/bin/tee --append $LOGFILE
		exit
	elif [ -s testDBConnection.txt ]
	then
		cat testDBConnection.txt 2>&1 | /usr/bin/tee --append $LOGFILE
		rm -v testDBConnection.txt testDBError.txt 2>&1 | /usr/bin/tee --append $LOGFILE
	fi


	mkdir -pv outputs

	GRAPHICS_REVISION=`head -1 /wres_share/releases/pendingQueueJ_wres-vis.txt | /usr/bin/sed -e s/wres-vis-//`
	WRESVIS_REVISION=`head -1 /wres_share/releases/pendingQueueJ_wres-vis.txt`
	echo "WRESVIS_REVISION = $WRESVIS_REVISION, GRAPHICS_REVISION = $GRAPHICS_REVISION" 2>&1 | /usr/bin/tee --append $LOGFILE

	echo "./gradlew cleanTest test -PversionToTest=$REVISION -PgraphicsVersionToTest=$GRAPHICS_REVISION -PwresZipDirectory=$wresZipDirectory -PwresGraphicsZipDirectory=$wresGraphicsZipDirectory -PtestJvmSystemProperties=\"-Dwres.useSSL=true -Dwres.username=$WRES_DB_USERNAMEJ -Dwres.url=$WRES_DB_HOSTNAMEJ -Dwres.databaseName=$WRES_DB_NAMEJ -Dwres.dataDirectory=/wres_share/testing -Dwres.logLevel=$WRES_LOG_LEVELJ -Djava.io.tmpdir=./outputs\ -Dwres.eventsBrokerAddress=localhost -Dwres.eventsBrokerPort=5673 -Dwres.externalGraphics=true" --tests=\"SystemTestSuite\" --$WRES_LOG_LEVELJ" 2>&1 | /usr/bin/tee --append $LOGFILE"
	./gradlew cleanTest test -PversionToTest=$REVISION -PgraphicsVersionToTest=$GRAPHICS_REVISION -PwresZipDirectory=$wresZipDirectory -PwresGraphicsZipDirectory=$wresGraphicsZipDirectory -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.username=$WRES_DB_USERNAMEJ -Dwres.url=$WRES_DB_HOSTNAMEJ -Dwres.databaseName=$WRES_DB_NAMEJ -Dwres.dataDirectory=. -Dwres.systemTestingSeed=2389187312693262 -Djava.io.tmpdir=./outputs -Dwres.eventsBrokerAddress=localhost -Dwres.eventsBrokerPort=5673 -Dwres.externalGraphics=true" --tests="SystemTestSuite" --$WRES_LOG_LEVELJ 2>&1 | /usr/bin/tee --append $LOGFILE
fi

LOGFILE_GRAPHICS=build/wres-graphics-${GRAPHICS_REVISION}.log
echo "LOGFILE_GRAPHICS = $LOGFILE_GRAPHICS" 2>&1 | /usr/bin/tee --append $LOGFILE
ls -l $LOGFILE_GRAPHICS 2>&1 | /usr/bin/tee --append $LOGFILE

(/usr/bin/grep class build/reports/tests/test/classes/*.html | grep failures | grep failed | cut -d'.' -f3-4 | tr -d ".html:c<t/d>" | sed -e s/ass\=\"faiures\"faie/"failed \!"/ > failures.txt) 2>> $LOGFILE
if [ -s failures.txt ]
then
	echo "/usr/bin/cat failures.txt" | /usr/bin/tee --append $LOGFILE
	/usr/bin/cat failures.txt | /usr/bin/tee --append $LOGFILE
else
	ls -l failures.txt | /usr/bin/tee --append $LOGFILE
fi
(/usr/bin/grep class build/reports/tests/test/classes/*.html | grep success | grep passed | cut -d'.' -f3-4 | tr -d ".html:c<t/d>"  | sed -e s/ass\=\"suess\"passe/passed/ > passes.txt) 2>> $LOGFILE
if [ -s passes.txt ]
then
	echo "/usr/bin/cat passes.txt" | /usr/bin/tee --append $LOGFILE
	/usr/bin/cat passes.txt | /usr/bin/tee --append $LOGFILE
else
	ls -l passes.txt | /usr/bin/tee --append $LOGFILE
fi
#/usr/bin/grep EXECUTING $LOGFILE | tr -d "#" | gawk '{printf("%s %s\n", $2,$3)}' > executed_order.txt
/usr/bin/grep EXECUTING $LOGFILE | tr -d "#" | gawk '{print($NF)}' | sed -e s/scenario/Scenario/ > executed_order.txt
if [ -s executed_order.txt ]
then
	echo "/usr/bin/cat executed_order.txt" | /usr/bin/tee --append $LOGFILE
	/usr/bin/cat executed_order.txt | /usr/bin/tee --append $LOGFILE
else
	ls -l executed_order.txt 2>&1 | /usr/bin/tee --append $LOGFILE
fi

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

/usr/bin/cat passes.txt failures.txt | sed -e s/Senario/Scenario/ > passed_failed.txt  2>&1 | /usr/bin/tee --append $LOGFILE
if [ -s passed_failed.txt ]
then
	echo "/usr/bin/cat passed_failed.txt" | /usr/bin/tee --append $LOGFILE
	/usr/bin/cat passed_failed.txt | /usr/bin/tee --append $LOGFILE
else
	ls -l  passed_failed.txt | /usr/bin/tee --append $LOGFILE
fi

#scenarios=`cat executed_order.txt | cut -d' ' -f2`
scenarios=`cat executed_order.txt`
echo "scenarios = $scenarios" | /usr/bin/tee --append $LOGFILE
cat /dev/null > summary.txt
/bin/df -h | /bin/grep wres_share > summary.txt
echo "Please notify the system administrator if the /wres_share file system close to 100% full!" >> summary.txt
echo "Ready to check the scenarios in passed_failed.txt" | /usr/bin/tee --append $LOGFILE
for scenario in $scenarios
do
	/usr/bin/grep $scenario passed_failed.txt >> summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE 
done
if [ -s summary.txt ]
then
	echo "/usr/bin/cat summary.txt" | /usr/bin/tee --append $LOGFILE
	/usr/bin/cat summary.txt | /usr/bin/tee --append $LOGFILE
else
	ls -l summary.txt | /usr/bin/tee --append $LOGFILE
fi

MAIL_SUBJECT="JUnit in $SYSTESTS_DIR Tested $WRES_REVISION with $WRESVIS_REVISION  : $pass_nums PASSED; $failure_nums FAILED"
LOGFILESIZE=`ls -s $LOGFILE | gawk '{print $1}'`
echo "LOGFILESIZE = $LOGFILESIZE" 2>&1 | /usr/bin/tee --append $LOGFILE
if [ $LOGFILESIZE -lt 9999 ]
then
# WRES Team prefer send the test result summary to Redmine ticket #89538 instead direct email to them. April, 2021 by RHC
# Below two lines are direct email summary
#	Note, WRES_Setting and WRES_GROUP variable are set in ~/.mailrc
#	/usr/bin/mailx -F -A WRES_Setting -s "$MAIL_SUBJECT" -a $LOGFILE -a $LOGFILE_GRAPHICS -v WRES_GROUP < summary.txt  2>&1 | /usr/bin/tee --append $LOGFILE
# --------------- when SMTP server is down, then uncomment below lines --------March, 2021 by RHC---------------
# Attached the email to Redmine ticket #89538

	echo "cat summary.txt" 2>&1 | /usr/bin/tee --append $LOGFILE
	cat summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE
	ls -l $WRES_DIRJ/install_scripts/redmineTemplateFile.xml 2>&1 | /usr/bin/tee --append $LOGFILE
	if [ -f $WRES_DIRJ/install_scripts/redmineTemplateFile.xml ]
	then
		cp -v $WRES_DIRJ/install_scripts/redmineTemplateFile.xml redmineTempFile.xml 2>&1 | /usr/bin/tee --append $LOGFILE 
		sed -i s/"TheSubjectLine"/"$MAIL_SUBJECT"/g redmineTempFile.xml
		cat summary.txt >> redmineTempFile.xml
		echo "</notes>" >> redmineTempFile.xml
		echo "</issue>" >> redmineTempFile.xml
		cat redmineTempFile.xml 2>&1 | /usr/bin/tee --append $LOGFILE 
		/usr/bin/curl -x '' -H 'X-Redmine***REMOVED***: ***REMOVED***' https://***REMOVED***/redmine/issues/89538.xml -X PUT -H 'Content-Type: text/xml' -T redmineTempFile.xml 
		rm -v redmineTempFile.xml 2>&1 | /usr/bin/tee --append $LOGFILE
	else
		ls -l $WRES_DIRJ/install_scripts/redmineTemplateFile.xml 2>&1 | /usr/bin/tee --append $LOGFILE
	fi

# -------- when SMTP server is down, then uncomment above lines ------------------------------
else
	echo "$LOGFILE block size $LOGFILESIZE is too large to attach in email" > tempfile.txt 2>&1 | /usr/bin/tee --append $LOGFILE
	echo ".................." >> tempfile.txt 2>&1 | /usr/bin/tee --append $LOGFILE
	cat summary.txt >> tempfile.txt 2>&1 | /usr/bin/tee --append $LOGFILE
	mv -v tempfile.txt summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE
#	/usr/bin/mailx -F -S smtp=nwcss-mail01.owp.nws.***REMOVED*** -s "$MAIL_SUBJECT" $WRES_GROUP < summary.txt  2>&1 | /usr/bin/tee --append $LOGFILE
	/usr/bin/mailx -F -A WRES_Setting -s "$MAIL_SUBJECT" -v WRES_GROUP < summary.txt  2>&1 | /usr/bin/tee --append $LOGFILE
fi
ls -l passes.txt failures.txt passed_failed.txt summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE
rm -v passes.txt failures.txt passed_failed.txt summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE
#rm -v redmineFile.txt 2>&1 | /usr/bin/tee --append $LOGFILE

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
