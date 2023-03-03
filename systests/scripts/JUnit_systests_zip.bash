#!/bin/bash

# Run the system tests with JUnit
# Author: Raymond.Chui@***REMOVED***
# Created: October, 2019

# source the WRES environment variables
#. WRES_Variables.txt 
. ~/.bash_profile
#. ~/.mailrc
cd ~

TIMESTAMP=$(/usr/bin/date +"%Y%m%d%H%M%S")
LOGDIR=/wres_share/releases/logs/junit_systests
LOGFILE=$LOGDIR/JUnit_systestsLog_${TIMESTAMP}.txt
TESTINGJ_LOCK_FILE=/wres_share/releases/install_scripts/testingJ.lock.txt
PENDINGQUEUEJ=/wres_share/releases/pendingQueueJ.txt
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk

# =====================================================
# Handle the lock file.  

# Check the system testing lock file.
if [ -f $TESTINGJ_LOCK_FILE ]
then
    ls -l $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE
    fileStatus=`/bin/stat $TESTINGJ_LOCK_FILE | grep Change | cut -d'.' -f1 | gawk '{print($2,$3)}'`
    lastHours=`/wres_share/releases/install_scripts/testDateTime.py "$fileStatus"`
    echo "The lock file, $TESTINGJ_LOCK_FILE, was created/changed $lastHours ago at $fileStatus." 2>&1 | /usr/bin/tee --append $LOGFILE
    if [ $lastHours -gt 2 ]
    then    # that testingJ.txt lock file had lasted for more than a hour, let's notice the developers by email 
        echo -n "System up time since last reboot: $(/bin/uptime -s)" 2>&1 | /usr/bin/tee --append $LOGFILE
        
        # Do not remove the lock file, let someone decide when to kill the previous test. 
        echo "The test has been last for $lastHours hours, still unfinished yet" | /usr/bin/tee --append $LOGFILE
        /usr/bin/mailx -F -A WRES_Setting -s "The test has been last for $lastHours hours, still unfinished yet" -v WRES_GROUP <<< `cat $TESTINGJ_LOCK_FILE`
        exit
    else
        echo "There is a system test running already running. Exiting without popping the test from the queue." 2>&1 | /usr/bin/tee --append $LOGFILE
        exit
    fi
fi

# If it gets past the locks, then the first thing we'll do is purge log files older than 30 days.
echo "Purging old log files from $LOGDIR."                                                    2>&1 | /usr/bin/tee --append $LOGFILE
/usr/bin/find -P $LOGDIR -maxdepth 1 -name "JUnit_systestsLog_*" -mtime +7 -exec rm -v {} \;  2>&1 | /usr/bin/tee --append $LOGFILE
echo "Purging old evaluation output directories across all system test revisions."            2>&1 | /usr/bin/tee --append $LOGFILE
/usr/bin/find -P /wres_share/releases -name "wres_evaluation_*" -mtime +7 -exec rm -rv {} \;  2>&1 | /usr/bin/tee --append $LOGFILE

# =========================================================
# Prepare for the system test execution.

# Establish the lock file.
touch $TESTINGJ_LOCK_FILE 
trap "rm -f $TESTINGJ_LOCK_FILE; echo ${date} - Lock file removed via trap. Queue file content: ; cat $PENDINGQUEUEJ" EXIT TERM INT KILL 

# Look at the queue for revisions to test.
if [ ! -s $PENDINGQUEUEJ ]
then
    # there isn't any queue
    echo "The queue file, $PENDINGQUEUEJ, is empty or does not exist; creating it, if necessary, and exiting."  2>&1 | /usr/bin/tee --append $LOGFILE
    touch $PENDINGQUEUEJ 2>&1 | /usr/bin/tee --append $LOGFILE
    ls -l $PENDINGQUEUEJ 2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE 
    exit
fi
revisionInfo=$(head -1 $PENDINGQUEUEJ)
read -r var1 var2 var3 <<< $revisionInfo
    
# Check the variables to make sure everything has a value.
# This only checks for an empty variable, not a properly formatted one.
if [ -z "$var1" -o -z "$var2" -o -z "$var3" ]
then
    echo "The revision line is improperly formated; 3 revisions including prefixes are required." 2>&1 | /usr/bin/tee --append $LOGFILE
    echo "The offending line = \"$revisionInfo\"."               2>&1 | /usr/bin/tee --append $LOGFILE
    echo "Removing the offending line from queue."               2>&1 | /usr/bin/tee --append $LOGFILE
    flock $PENDINGQUEUEJ /usr/bin/sed  -i '1d' $PENDINGQUEUEJ    2>&1 | /usr/bin/tee --append $LOGFILE
    echo "The queue is now:"                                     2>&1 | /usr/bin/tee --append $LOGFILE
    echo "----------"                                            2>&1 | /usr/bin/tee --append $LOGFILE
    cat -n $PENDINGQUEUEJ                                        2>&1 | /usr/bin/tee --append $LOGFILE
    echo "----------"                                            2>&1 | /usr/bin/tee --append $LOGFILE
    echo "Exiting..."                                            2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE                                    2>&1 | /usr/bin/tee --append $LOGFILE
    exit 2
fi

# Record the revisions.
coreRevision=$var1
echo "Revision number = $coreRevision" 2>&1 | /usr/bin/tee --append $LOGFILE 
wresVisRevision=$var2
echo "wres-vis revision number = $wresVisRevision" 2>&1 | /usr/bin/tee --append $LOGFILE
systestsRevision=$var3
echo "Systests revision number = $systestsRevision" 2>&1 | /usr/bin/tee --append $LOGFILE


echo "################ $TIMESTAMP #######################" 2>&1 | /usr/bin/tee --append $LOGFILE

# ====================================================================
# Check for expected environment variables to support the run.
# If one is not found, do not remove the system test from the queue. Just log it and then remove the ock file.
if [ -z "$WRES_DB_NAMEJ" ]
then
    echo "Please export WRES_DB_NAMEJ in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE 
    exit 2 
else
    echo "WRES_DB_NAMEJ = $WRES_DB_NAMEJ"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_LOG_LEVELJ" ]
then
	export WRES_LOG_LEVELJ=info
fi
echo "WRES_LOG_LEVELJ = $WRES_LOG_LEVELJ"  2>&1 | /usr/bin/tee --append $LOGFILE
if [ -z "$WRES_DB_USERNAMEJ" ]
then
    echo "Please export WRES_DB_USERNAMEJ in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE 
    exit 2
else
    echo "WRES_DB_USERNAMEJ = $WRES_DB_USERNAMEJ"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_DB_HOSTNAMEJ" ]
then
    echo "Please export WRES_DB_HOSTNAMEJ in ~/.bash_profile"  2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE 
    exit 2
else
    echo "WRES_DB_HOSTNAMEJ = $WRES_DB_HOSTNAMEJ"  2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ ! -s ~/.pgpass ]
then
    echo "Please enter WRES_DB_HOSTNAMEJ:PORT:WRES_DB_NAMEJ:WRES_DB_USERJ:{password} in ~/.pgpass"  2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE 
    exit 2
else
    ls -l ~/.pgpass 2>&1 | /usr/bin/tee --append $LOGFILE
fi
if [ -z "$WRES_DIRJ" ]
then
    echo "Please export WRES_DIRJ in ~/.bash_profile" 2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE 
    exit 2
else
    cd $WRES_DIRJ
    echo -n "WRES_DIRJ: " 2>&1 | /usr/bin/tee --append $LOGFILE
    /usr/bin/pwd 2>&1 | /usr/bin/tee --append $LOGFILE	
fi

# Confirm the expected .zip files are found, as well as the system testing directory.
wresZipDirectory=/wres_share/releases/archive
if [ ! -s $wresZipDirectory/$coreRevision.zip ]
then 
    echo "The wres revision .zip file, $wresZipDirectory/$coreRevision.zip, not found. Not good!"  2>&1 | /usr/bin/tee --append $LOGFILE
    echo "Removing the revision info, \"$revisionInfo\", from the queue and exiting."              2>&1 | /usr/bin/tee --append $LOGFILE
    /usr/bin/sed  -i '1d' $PENDINGQUEUEJ                                                           2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE                                2>&1 | /usr/bin/tee --append $LOGFILE 
    exit
fi
wresVisZipDirectory=/wres_share/releases/archive/graphics
if [ ! -s $wresVisZipDirectory/$wresVisRevision.zip ]
then  
    echo "The wres-vis revision .zip file, $wresVisZipDirectory/$wresVisRevision.zip, not found. Not good!" 2>&1 | /usr/bin/tee --append $LOGFILE
    echo "Removing the revision info, \"$revisionInfo\", from the queue and exiting."      i                2>&1 | /usr/bin/tee --append $LOGFILE
    /usr/bin/sed  -i '1d' $PENDINGQUEUEJ                                                                    2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE                                         2>&1 | /usr/bin/tee --append $LOGFILE
    exit 
fi
if [ ! -d $WRES_DIRJ/$systestsRevision ]
then
    echo "The systests directory, $WRES_DIRJ/$systestsRevision, not found. Not good!"            2>&1 | /usr/bin/tee --append $LOGFILE
    echo "Removing the revision info, \"$revisionInfo\", from the queue and exiting."            2>&1 | /usr/bin/tee --append $LOGFILE
    /usr/bin/sed  -i '1d' $PENDINGQUEUEJ                                                         2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE                              2>&1 | /usr/bin/tee --append $LOGFILE
fi

# Log the information.
echo "Core wres .zip is $wresZipDirectory/$coreRevision.zip"      2>&1 | /usr/bin/tee --append $LOGFILE
echo "wres-vis .zip is $wresVizZipDirectory/$wresVisRevision.zip" 2>&1 | /usr/bin/tee --append $LOGFILE
echo "systests run directory is $WRES_DIRJ/$systestsRevision"     2>&1 | /usr/bin/tee --append $LOGFILE
SYSTESTS_DIR=$WRES_DIRJ/$systestsRevision

# Go to the systests directory.
echo "Changing to the systests directory, $SYSTESTS_DIR" 2>&1 | /usr/bin/tee --append $LOGFILE
cd $SYSTESTS_DIR
/usr/bin/pwd 2>&1 | /usr/bin/tee --append $LOGFILE

# Establish the data link.
if [ ! -L data ]
then    # if the data link doesn't exist, then create it
    ls -l data 2>&1 | /usr/bin/tee --append $LOGFILE
    ln -sv /wres_share/testing/data data 2>&1 | /usr/bin/tee --append $LOGFILE
fi
	
# Confirm a database connection!
/bin/psql --host $WRES_DB_HOSTNAMEJ --dbname $WRES_DB_NAMEJ --username $WRES_DB_USERNAMEJ --list --output testDBConnection.txt 2> testDBError.txt
ls -l testDBConnection.txt testDBError.txt 2>&1 | /usr/bin/tee --append $LOGFILE
if [ -s testDBError.txt ]
then
    MAIL_SUBJECT="Dudes, JUnit in $SYSTESTS_DIR Tested $coreRevision with Database problem"
    /usr/bin/mailx -F -A WRES_Setting -s "$MAIL_SUBJECT" -v WRES_GROUP < testDBError.txt  2>&1 | /usr/bin/tee --append $LOGFILE
    cat testDBError.txt 2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v testDBError.txt testDBError.txt 2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE
    exit
elif [ -s testDBConnection.txt ]
then
    cat testDBConnection.txt 2>&1 | /usr/bin/tee --append $LOGFILE
    rm -v testDBConnection.txt testDBError.txt 2>&1 | /usr/bin/tee --append $LOGFILE
fi

# Create the outputs directory, noting this will do nothing if it exists.
mkdir -pv outputs

# The JUnit Gradle run does not expect the "wres-" and "wres-vis-" prefixes for the revisions.
# We need to get the identifiers only.
wresVisRevisionId=$(echo $wresVisRevision | /usr/bin/sed -e s/wres-vis-//)
coreRevisionId=$(echo $coreRevision | /usr/bin/sed -e s/wres-//)
echo "The wres vis revision identifier is $wresVisRevisionId." 2>&1 | /usr/bin/tee --append $LOGFILE
echo "The core revision identifier is $coreRevisionId"         2>&1 | /usr/bin/tee --append $LOGFILE
echo "$revisionInfo" >> $TESTINGJ_LOCK_FILE

# =========================================
# Run the system tests!
echo "Executing the system tests using this command:" 2>&1 | /usr/bin/tee --append $LOGFILE
echo "" 2>&1 | /usr/bin/tee --append $LOGFILE
echo "./gradlew cleanTest test -PversionToTest=$coreRevisionId -PgraphicsVersionToTest=$wresVisRevisionId -PwresZipDirectory=$wresZipDirectory -PwresGraphicsZipDirectory=$wresVisZipDirectory -PtestJvmSystemProperties=\"-Dwres.useDatabase=true -Dwres.useSSL=true -Dwres.username=$WRES_DB_USERNAMEJ -Dwres.url=$WRES_DB_HOSTNAMEJ -Dwres.databaseName=$WRES_DB_NAMEJ -Dwres.dataDirectory=/wres_share/testing -Dwres.logLevel=$WRES_LOG_LEVELJ -Djava.io.tmpdir=./outputs -Dwres.eventsBrokerAddress=localhost -Dwres.eventsBrokerPort=5673 -Dwres.externalGraphics=true -Dwres.startBroker=false\" -PtestJvmSystemPropertiesGraphics=\"-Djava.io.tmpdir=./outputs -Dwres.eventsBrokerAddress=localhost -Dwres.eventsBrokerPort=5673 -Dwres.startBroker=true\" --tests=\"SystemTestSuite\" --$WRES_LOG_LEVELJ 2>&1 | /usr/bin/tee --append $LOGFILE"

echo"" 2>&1 | /usr/bin/tee --append $LOGFILE 
echo "System test now executing *********************************" 2>&1 | /usr/bin/tee --append $LOGFILE
./gradlew cleanTest test -PversionToTest=$coreRevisionId -PgraphicsVersionToTest=$wresVisRevisionId -PwresZipDirectory=$wresZipDirectory -PwresGraphicsZipDirectory=$wresVisZipDirectory -PtestJvmSystemProperties="-Dwres.useDatabase=true -Dwres.useSSL=true -Dwres.username=$WRES_DB_USERNAMEJ -Dwres.url=$WRES_DB_HOSTNAMEJ -Dwres.databaseName=$WRES_DB_NAMEJ -Dwres.dataDirectory=. -Dwres.systemTestingSeed=2389187312693262 -Djava.io.tmpdir=./outputs -Dwres.eventsBrokerAddress=localhost -Dwres.eventsBrokerPort=5673 -Dwres.externalGraphics=true -Dwres.startBroker=false" -PtestJvmSystemPropertiesGraphics="-Djava.io.tmpdir=./outputs -Dwres.eventsBrokerAddress=localhost -Dwres.eventsBrokerPort=5673 -Dwres.startBroker=true" --tests="SystemTestSuite" --$WRES_LOG_LEVELJ 2>&1 | /usr/bin/tee --append $LOGFILE
echo "System testing complete ***********************************" 2>&1 | /usr/bin/tee --append $LOGFILE
echo "" 2>&1 | /usr/bin/tee --append $LOGFILE

# ========================================
# Process the results.

# Identify the graphics/wres-vis logs.
LOGFILE_GRAPHICS=build/wres-graphics-${wresVisRevisionId}.log
echo "LOGFILE_GRAPHICS = $LOGFILE_GRAPHICS" 2>&1 | /usr/bin/tee --append $LOGFILE
ls -l $LOGFILE_GRAPHICS                     2>&1 | /usr/bin/tee --append $LOGFILE

# Grep out the failures and successes.
(/usr/bin/grep class build/reports/tests/test/classes/*.html | grep failures | grep failed | cut -d'.' -f3-4 | tr -d ".html:c<t/d>" | sed -e s/ass\=\"faiures\"faie/"failed \!"/ > failures.txt) 2>> $LOGFILE
if [ -s failures.txt ]
then
    echo "/usr/bin/cat failures.txt" | /usr/bin/tee --append $LOGFILE
    /usr/bin/cat failures.txt        | /usr/bin/tee --append $LOGFILE
else
    ls -l failures.txt               | /usr/bin/tee --append $LOGFILE
fi
(/usr/bin/grep class build/reports/tests/test/classes/*.html | grep success | grep passed | cut -d'.' -f3-4 | tr -d ".html:c<t/d>"  | sed -e s/ass\=\"suess\"passe/passed/ > passes.txt) 2>> $LOGFILE
if [ -s passes.txt ]
then
    echo "/usr/bin/cat passes.txt"   | /usr/bin/tee --append $LOGFILE
    /usr/bin/cat passes.txt          | /usr/bin/tee --append $LOGFILE
else
    ls -l passes.txt                 | /usr/bin/tee --append $LOGFILE
fi

# Grep out the exeuction order.
/usr/bin/grep EXECUTING $LOGFILE | tr -d "#" | gawk '{print($NF)}' | sed -e s/scenario/Scenario/ > executed_order.txt
if [ -s executed_order.txt ]
then
    echo "/usr/bin/cat executed_order.txt" | /usr/bin/tee --append $LOGFILE
    /usr/bin/cat executed_order.txt        | /usr/bin/tee --append $LOGFILE
else
    ls -l executed_order.txt 2>&1          | /usr/bin/tee --append $LOGFILE
fi

# Count the number of failures and passes.
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

# Put the passes and failures into one file.
/usr/bin/cat passes.txt failures.txt | sed -e s/Senario/Scenario/ > passed_failed.txt  2>&1 | /usr/bin/tee --append $LOGFILE
if [ -s passed_failed.txt ]
then
    echo "/usr/bin/cat passed_failed.txt" | /usr/bin/tee --append $LOGFILE
    /usr/bin/cat passed_failed.txt        | /usr/bin/tee --append $LOGFILE
else
    ls -l  passed_failed.txt              | /usr/bin/tee --append $LOGFILE
fi

# More messaging. 
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
    /usr/bin/cat summary.txt        | /usr/bin/tee --append $LOGFILE
else
    ls -l summary.txt               | /usr/bin/tee --append $LOGFILE
fi

MAIL_SUBJECT="JUnit in $SYSTESTS_DIR Tested $coreRevisionId with $wresVisRevisionId  : $pass_nums PASSED; $failure_nums FAILED"
LOGFILESIZE=`ls -s $LOGFILE | gawk '{print $1}'`
echo "LOGFILESIZE block size = $LOGFILESIZE" 2>&1 | /usr/bin/tee --append $LOGFILE
#if [ $LOGFILESIZE -lt 9999 ]
if [ $LOGFILESIZE -lt 9999999 ]
then
# WRES Team prefer send the test result summary to Redmine ticket #89538 instead direct email to them. April, 2021 by RHC
# Below two lines are direct email summary
#	Note, WRES_Setting and WRES_GROUP variable are set in ~/.mailrc
#	/usr/bin/mailx -F -A WRES_Setting -s "$MAIL_SUBJECT" -a $LOGFILE -a $LOGFILE_GRAPHICS -v WRES_GROUP < summary.txt  2>&1 | /usr/bin/tee --append $LOGFILE
# --------------- when SMTP server is down, then uncomment below lines --------March, 2021 by RHC---------------
# Attached the email to Redmine ticket #89538

    echo "cat summary.txt" 2>&1 | /usr/bin/tee --append $LOGFILE
    cat summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE

    echo "<?xml version=\"1.0\" ?><issue><subject>" > redmineTempFile.xml
    echo "$MAIL_SUBJECT" >> redmineTempFile.xml
    echo "</subject><notes>" >> redmineTempFile.xml
    cat summary.txt >> redmineTempFile.xml
    echo "</notes>" >> redmineTempFile.xml
    # pass the log files and output XML file to below script, it will zip the log files, get their tokens, append all
    # neccessary XML element tags, then upload the zip files and update the Redmine ticket #89538
    $WRES_DIRJ/install_scripts/getPostFileToken.bash ${LOGFILE} ${LOGFILE_GRAPHICS} redmineTempFile.xml 2>&1 | /usr/bin/tee --append $LOGFILE 

    rm -v redmineTempFile.xml 2>&1 | /usr/bin/tee --append $LOGFILE

# -------- when SMTP server is down, then uncomment above lines ------------------------------
else
    echo "$LOGFILE block size $LOGFILESIZE is too large to attach in email" > tempfile.txt 2>&1 | /usr/bin/tee --append $LOGFILE
    echo ".................." >> tempfile.txt 2>&1 | /usr/bin/tee --append $LOGFILE
    cat summary.txt >> tempfile.txt 2>&1 | /usr/bin/tee --append $LOGFILE
    mv -v tempfile.txt summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE
    /usr/bin/mailx -F -A WRES_Setting -s "$MAIL_SUBJECT" -v WRES_GROUP < summary.txt  2>&1 | /usr/bin/tee --append $LOGFILE
fi
ls -l passes.txt failures.txt passed_failed.txt summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE
rm -v passes.txt failures.txt passed_failed.txt summary.txt 2>&1 | /usr/bin/tee --append $LOGFILE

# remove JUnit test lock file
rm -v $TESTINGJ_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE 

# delete the top (1st) line of JUnit pending queue
echo "Remove $REVISION from $PENDINGQUEUEJ" 2>&1 | /usr/bin/tee --append $LOGFILE
/usr/bin/sed  -i '1d' $PENDINGQUEUEJ 2>&1 | /usr/bin/tee --append $LOGFILE

if [ -d outputs ]
then
    cd outputs
    /usr/bin/pwd 2>&1 | /usr/bin/tee --append $LOGFILE	
    # remove outout archive older than 1 days.
    find -P . -maxdepth 1 -name "wres_evaluation_*" -mtime +1 -exec rm -rf {} \; 2>&1 | /usr/bin/tee --append $LOGFILE
else
    ls -d outputs 2>&1 | /usr/bin/tee --append $LOGFILE
fi
