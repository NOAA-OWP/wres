#!/bin/bash

# Get the recent built zip file from Jenkins and install it
# Author: Raymond.Chui@***REMOVED***
# Created: December, 2017
#
# Arguments: First argument is a config file (any name), add a line with
#	user = firstName.lastName:passwd
#	Where the login ID and passwd are from https://vlab.***REMOVED***
#		Second argument is whether install the recent built, with value yes or no(default).
#
# Required Files: read_build.py, read_revision.py
# Output Files: build_history.json, build_revision.json
#		screenCapture.txt under your ~/wres_testing/wres/systests directory
# Downloaded Files: core WRES .zip, systests .zip, wres-vis .zip, all identified from Jenkins revision.

if [ $# -lt 1 ]
then
    echo "Usage: $0 config_file [yes|no]"
    echo "config_file with a line 'user = FirstName.LastName:passwd'"
    exit 2
fi

# Important constants
CURL=/usr/bin/curl
TODAY=`/usr/bin/date +"%Y-%m-%d"`
LOGFILE=/wres_share/releases/logs/install/installBuiltLog_$TODAY.txt
HOUR=$(date "+%_H")
MINUTE=$(date "+%_M")
NUMBER_RE='^[0-9]+$' # Regular expression to confirm something has only digits.

# Important URLs.
LATEST_URLSITE=https://vlab.***REMOVED***/jenkins/job/Verify_OWP_WRES/ws/build/distributions
SYSTESTS_URLSITE=https://vlab.***REMOVED***/jenkins/job/Verify_OWP_WRES/ws/systests/build/distributions
WRESVIS_URLSITE=https://vlab.***REMOVED***/jenkins/job/Verify_OWP_WRES/ws/wres-vis/build/distributions

# Other important stuff
INSTALLING_LOCK_FILE=/wres_share/releases/systests/installing.lock

# What is this for?
if [ $HOUR -le 0 -a $MINUTE -lt 5 ]
then
    echo "HERE1"
    cat /dev/null > $LOGFILE
fi

# Log the date.
echo "" >> $LOGFILE
echo "------------------------------------------- " >> $LOGFILE
echo "RUNNING INSTALL SCRIPT - $(/usr/bin/date)" >> $LOGFILE
echo "-------------------------------------------" >> $LOGFILE

# Check for another install run on-going by looking at the lock file.
if [ -f $INSTALLING_LOCK_FILE ]
then
    ls -l $INSTALLING_LOCK_FILE  2>&1 >> $LOGFILE 2>&1
    
    # Check the age of the lock file.  
    fileStatus=`/bin/stat $INSTALLING_LOCK_FILE | grep Change | cut -d'.' -f1 | gawk '{print($2,$3)}'`
    lastHours=`/wres_share/releases/install_scripts/testDateTime.py "$fileStatus"`
    echo "A release is currently being installed." 2>&1 >> $LOGFILE 2>&1
    if [ $lastHours -gt 12 ]
    then 
        # The lock file had lasted for more than 12 hours. The process likely died without removing it.
        # Forcibly remove the lock file.
        echo "The install has been on-going for $lastHours hours. Assuming its a dead process and removing the lock file." >> $LOGFILE 2>&1
#        /usr/bin/mailx -F -A WRES_Setting -s "The install has been last for $lastHours hours, still unfinished yet" -v WRES_GROUP <<< `cat $INSTALLING_LOCK_FILE`
        rm -v $INSTALLING_LOCK_FILE 2>&1 | /usr/bin/tee --append $LOGFILE
        exit
    else
        echo "Exiting install script." 2>&1 >> $LOGFILE 2>&1
        exit
    fi
fi

# Lock the install to let others know this is running.
echo "Establishing lock file, $INSTALLING_LOCK_FILE." >> $LOGFILE 2>&1
touch $INSTALLING_LOCK_FILE
ls -l $INSTALLING_LOCK_FILE >> $LOGFILE 2>&1
trap "rm -fv $INSTALLING_LOCK_FILE >> $LOGFILE 2>&1; echo 'Install lock file removed with script exit.' >> $LOGFILE 2>&1 " EXIT TERM INT KILL

# Purge log files older than 5 days.
/usr/bin/find -P $RELEASES_DIR/logs/install -name "installBuiltLog_*" -mtime +2 -exec rm -v {} \; >> $LOGFILE 2>&1

# Initialize some tracking variables.
doSystests=no
CONFIGFILE=$1
if [ $# -gt 1 ]
then
    INSTALL_Latest=$2
    INSTALL_Systests=$2
    INSTALL_WresVis=$2
else
    INSTALL_Latest=no
    INSTALL_Systests=no
    INSTALL_WresVis=no
fi

echo "Install built release?: $INSTALL_Latest"  >> $LOGFILE 2>&1
CURL=/usr/bin/curl

# Important directories that are assumed to exist already.
PYTHON_SCRIPT_DIR=/wres_share/releases/install_scripts
WORKDIR=~ #The wres-cron home directory is the working directory.
RELEASES_DIR=/wres_share/releases
SYSTESTS_DIR=/wres_share/releases/systests

# Ensure these directories exist.  
ARCHIVE_DIR=/wres_share/releases/archive
if [ ! -d $ARCHIVE_DIR ] 
then 
    mkdir -pv $ARCHIVE_DIR >> $LOGFILE 2>&1
fi
SYSTESTS_ARCHIVE_DIR=/wres_share/releases/systests_archive
if [ ! -d $SYSTESTS_ARCHIVE_DIR ]
then
    mkdir -pv $SYSTESTS_ARCHIVE >> $LOGFILE 2>&1
fi 
WRES_VIS_ARCHIVE_DIR=/wres_share/releases/archive/graphics
if [ ! -d $WRES_VIS_ARCHIVE_DIR ]
then
    mkdir -pv $WRES_VIS_ARCHIVE_DIR >> $LOGFILE 2>&1
fi

# the working directory is at your home directory
cd $WORKDIR 
pwd >> $LOGFILE

# ========================================
# STEP 1: Get the Jenkins build number from build history.

# Remove the old file Jenkins build history JSON file.
if [ -f build_history.json ]
then
   rm -v build_history.json >> $LOGFILE 2>&1
fi 
 
# Curl to get the new JSON. Check for success and then parse using the Python script.
$CURL --config $CONFIGFILE --silent  https://vlab.***REMOVED***/jenkins/job/Verify_OWP_WRES/api/json?pretty=true --output "build_history.json" 
if [ ! -s build_history.json ] # if download unsuccessful...
then
    # Log what ls sees. Remove the installing signal file. Exit 2.
    ls -l build_history.json >> $LOGFILE 2>&1
    rm -v $INSTALLING_LOCK_FILE >> $LOGFILE 2>&1
    exit 2
fi

# Python script extracts the build number and returns it.
builtNumber=$( $PYTHON_SCRIPT_DIR/read_build.py build_history.json )
if ! [[ $builtNumber =~ $NUMBER_RE ]] # Make sure its a valid build number before using.
then
    echo "Build number from Jenkins is not a number: \"$builtNumber\". NOT GOOD. Exiting."  >> $LOGFILE 2>&1
    exit 2
fi

echo "Recent build number = $builtNumber" >> $LOGFILE 2>&1

# =======================================
# STEP 2: Get the revision number and .zip file names.

# Remove the old revision file
if [ -f build_revision.json ]
then
    rm -v build_revision.json >> $LOGFILE 2>&1
fi

# Get the revision information JSON from Jenkins and confirm.
$CURL --config $CONFIGFILE --silent  https://vlab.***REMOVED***/jenkins/job/Verify_OWP_WRES/$builtNumber/api/json?pretty=true --output "build_revision.json" 
if [ ! -s build_revision.json ] # if this file is unsuccessed downlowd.
then
    # Log what ls sees.  Remove the installing signal file. Exit 2.
    ls -l build_revision.json >> $LOGFILE 2>&1
    rm -v $INSTALLING_LOCK_FILE >> $LOGFILE 2>&1
    exit 2
fi

# Obtain the revision number and zip file names through the Python script.
revisionInfo=$( $PYTHON_SCRIPT_DIR/read_revision.py build_revision.json )
read -r var1 var2 var3 var4 <<< $revisionInfo

# Check every variable before using.
# var1, the revision number, cannot be empty; that's the only requirement.
if [ -z "$var1" ]
then
    echo "The revision number was empty. NOT GOOD."  >> $LOGFILE 2>&1
    echo "Here is the response of read_revision.py build_revision.json:" >> $LOGFILE 2>&1
    echo "$revisionInfo" >> $LOGFILE 2>&1
    echo "Exiting..." >> $LOGFILE 2>&1
    exit 2
fi
revisionNumber=$var1
echo "Revision number = $revisionNumber"  >> $LOGFILE 2>&1 

# Obtain the sub-revision number which is the first 7 characters of the revision.
sub_revision=$(echo $revisionNumber | cut -c1-7)
echo "Sub-revision number = $sub_revision" >> $LOGFILE 2>&1

# var2, var3, and var4 need to end with ".zip". I assume they are good if they do.
if [[ $var2 != *.zip ]]
then
   echo "The revision WRES core zip file does not end in \".zip\". NOT GOOD." >> $LOGFILE 2>&1
   echo "Here is the response of read_revision.py build_revision.json:" >> $LOGFILE 2>&1
   echo "$revisionInfo" >> $LOGFILE 2>&1
   echo "Exiting..." >> $LOGFILE 2>&1
   exit 2
fi
LATEST_ZIPFILE=$var2
echo "WRES Core Zip File = $LATEST_ZIPFILE" >> $LOGFILE 2>&1
if [[ $var3 != *.zip ]]
then 
   echo "The revision systests zip file does not end in \".zip\". NOT GOOD." >> $LOGFILE 2>&1
   echo "Here is the response of read_revision.py build_revision.json:" >> $LOGFILE 2>&1
   echo "$revisionInfo" >> $LOGFILE 2>&1
   echo "Exiting..." >> $LOGFILE 2>&1
   exit 2 
fi
SYSTESTS_ZIPFILE=$var3
echo "Systest Zip File = $SYSTESTS_ZIPFILE" >> $LOGFILE 2>&1
if [[ $var4 != *.zip ]]
then
   echo "The revision wres-vis zip file does not end in \".zip\". NOT GOOD." >> $LOGFILE 2>&1
   echo "Here is the response of read_revision.py build_revision.json:" >> $LOGFILE 2>&1
   echo "$revisionInfo" >> $LOGFILE 2>&1
   echo "Exiting..." >> $LOGFILE 2>&1
   exit 2
fi
WRES_VIS_ZIPFILE=$var4
echo "Wres-vis Zip File = $WRES_VIS_ZIPFILE" >> $LOGFILE 2>&1

# Identify the revisions, which are the same as the zip files without the .zip extensions.
latest_noZip=`echo $LATEST_ZIPFILE | tr -d ".zip"`
wresvis_noZip=`echo $WRES_VIS_ZIPFILE | sed -e s/\.zip//`
systests_noZip=`echo $SYSTESTS_ZIPFILE | tr -d ".zip"`

echo "Here the three revisions: $latest_noZip, $wresvis_noZip, $systest_noZip" >> $LOGFILE 2>&1

# ==============================================
# STEP 3: Download and install each of the zip files appropriately. 

# Get the name of the zip file without the .zip extension.
# Check if the build has already been installed.
if [ -n "$LATEST_ZIPFILE" ]
then
    unZip_Latest=`echo $LATEST_ZIPFILE | tr -d ".zip"`
    if [ -d $RELEASES_DIR/$unZip_Latest ]
    then
        echo "Already installed $RELEASES_DIR/$unZip_Latest:" >> $LOGFILE 2>&1
        ls -ld $RELEASES_DIR/$unZip_Latest >> $LOGFILE 2>&1
	INSTALL_Latest=no
    fi
elif [ -z "$LATEST_ZIPFILE" ]
then
    INSTALL_Latest=no
fi

# Do the same thing with the systest zip file.  Has it been installed?
if [ -n "$SYSTESTS_ZIPFILE" ]
then
    unZip_SysTests=`echo $SYSTESTS_ZIPFILE | tr -d ".zip"`
    if [ -d $RELEASES_DIR/$unZip_SysTests ]
    then 
        echo "Already installed $RELEASES_DIR/$unZip_SysTests:" >> $LOGFILE 2>&1
        ls -ld $RELEASES_DIR/$unZip_SysTests >> $LOGFILE 2>&1
        INSTALL_Systests=no
    fi
elif [ -z "$SYSTESTS_ZIPFILE" ]
then
    INSTALL_Systests=no	
fi

# Do the same thing with the wres-vis zip file.  Has it been installed?
# In this case, we are looking for an archived .zip; its not unzipped to a directory.
if [ -n "$WRES_VIS_ZIPFILE" ]
then
    if [ -f $WRES_VIS_ARCHIVE_DIR/$WRES_VIS_ZIPFILE ]
    then
        echo "Already installed $WRES_VIS_ARCHIVE_DIR/$WRES_VIS_ZIPFILE:" >> $LOGFILE 2>&1
        ls -ld $WRES_VIS_ARCHIVE_DIR/$WRES_VIS_ZIPFILE >> $LOGFILE 2>&1
        INSTALL_WresVis=no
    fi
elif [ -z "$WRES_VIS_ZIPFILE" ]
then
    INSTALL_WresVis=no
fi

# =============================================
# STEP 4: Download the zip files if the install flag is 'yes'.
if [ "$INSTALL_Latest" = "yes" ]
then
    echo "Please wait .... currently is downloading the $LATEST_ZIPFILE file" >> $LOGFILE 2>&1
    $CURL --config $CONFIGFILE --silent --remote-name $LATEST_URLSITE/$LATEST_ZIPFILE 
fi
if [ "$INSTALL_Systests" = "yes" ]
then
    echo "Please wait .... currently is downloading the $SYSTESTS_ZIPFILE file" >> $LOGFILE 2>&1
    $CURL --config $CONFIGFILE --silent --remote-name $SYSTESTS_URLSITE/$SYSTESTS_ZIPFILE 
fi
if [ "$INSTALL_WresVis" = "yes" ]
then
    echo "Please wait .... currently is downloading the $WRES_VIS_ZIPFILE file" >> $LOGFILE 2>&1
    $CURL --config $CONFIGFILE --silent --remote-name $WRESVIS_URLSITE/$WRES_VIS_ZIPFILE
fi

# =====================================
# STEP 5: Install the .zip files. 
# The core and systests are unzipped with the .zip archived.  The wres-vis is just archived.

# Core zip.  This is installed using the script and then the .zip is archived.
if [ -n "$LATEST_ZIPFILE" -a -f $LATEST_ZIPFILE ] # if the recent built zip file has downloaded
then
    ls -l $LATEST_ZIPFILE >> $LOGFILE 2>&1
    echo "install built file is $noZip" >> $LOGFILE 2>&1
    if [ "$INSTALL_Latest" = "yes" ]
    then
        $RELEASES_DIR/install_scripts/installrls.fromzip.bash $latest_noZip >> $LOGFILE 2>&1
        mv -v $LATEST_ZIPFILE $ARCHIVE_DIR >> $LOGFILE 2>&1
        /usr/bin/chgrp -v wres $ARCHIVE_DIR/$LATEST_ZIPFILE >> $LOGFILE 2>&1
        chmod 775 $ARCHIVE_DIR/$LATEST_ZIPFILE >> $LOGFILE 2>&1
        echo "$builtNumber # $latest_noZip" >> $ARCHIVE_DIR/recordDownloadedNBuilt.txt
        doSystests=yes
    fi
fi

# Systests zip. This is installed using the script and then the .zip is archived.  
if [ -n "$SYSTESTS_ZIPFILE" ] # if the variable is defined
then
    if [ -f $SYSTESTS_ZIPFILE ] # if the recent systests zip file has downloaded
    then
        ls -l $SYSTESTS_ZIPFILE >> $LOGFILE 2>&1
        echo "INSTALL_Systests = $INSTALL_Systests " >> $LOGFILE 2>&1
        if [ "$INSTALL_Systests" = "yes" ]
        then
            $RELEASES_DIR/install_scripts/installsystests.fromzip.bash $systests_noZip >> $LOGFILE 2>&1
            mv -v $SYSTESTS_ZIPFILE $SYSTESTS_ARCHIVE_DIR >> $LOGFILE 2>&1
            /usr/bin/chgrp -v wres $SYSTESTS_ARCHIVE_DIR/$SYSTESTS_ZIPFILE >> $LOGFILE 2>&1	
            chmod 775 $SYSTESTS_ARCHIVE_DIR/$SYSTESTS_ZIPFILE >> $LOGFILE 2>&1
            echo "$builtNumber # $systests_noZip" >> $SYSTESTS_ARCHIVE_DIR/recordDownloadedNBuilt.txt
            doSystests=yes
        elif [ "$INSTALL_Systests" = "no" ]
        then
            echo "The previous systeests perhaps still installing" >> $LOGFILE 2>&1
            rm -v $SYSTESTS_ZIPFILE >> $LOGFILE 2>&1
            #doSystests=no
        fi
    fi
fi

# Wres-vis zip. This one is not unzipped, but copied into place.
if [ -n "$WRES_VIS_ZIPFILE" ] # if the variable is defined
then
    if [ -f "$WRES_VIS_ZIPFILE" ] # if the wres-vis file was downloaded
    then
        ls -l $WRES_VIS_ZIPFILE >> $LOGFILE 2>&1
        echo "Wres Viz file witout .zip = $wresvis_noZip" >> $LOGFILE 2>&1
        echo "INSTALL_WresVis = $INSTALL_WresVis" >> $LOGFILE 2>&1
        if [ "$INSTALL_WresVis" = "yes" ]
        then
            echo "Moving $WRES_VIS_ZIPFILE to $WRES_VIS_ARCHIVE_DIR; it will not be unzipped." >> $LOGFILE 2>&1
            mv $WRES_VIS_ZIPFILE $WRES_VIS_ARCHIVE_DIR/.
            /usr/bin/chgrp -v wres $WRES_VIS_ARCHIVE_DIR/$WRES_VIS_ZIPFILE >> $LOGFILE 2>&1
            chmod 775 $WRES_VIS_ARCHIVE_DIR/$WRES_VIS_ZIPFILE >> $LOGFILE 2>&1
        else
            rm $WRES_VIS_ZIPFILE # Just remove the file; its already been installed.
        fi
    fi
fi

# =========================================
# STEP 5: Queue the revision for testing if the flag is a yes.
# This conists of adding the core revision and wres-vis revision in queues.
echo "INSTALL_Latest = $INSTALL_Latest, INSTALL_Systests = $INSTALL_Systests" >> $LOGFILE 2>&1
if [ "$INSTALL_Latest" = "yes" -o "$INSTALL_Systests" = "yes" ]
then
    doSystests=yes
fi
echo "doSystests =  $doSystests" >> $LOGFILE 2>&1

if [ "$doSystests" = "yes" ]
then
    # append the latest revision number into the queue
    flock $RELEASES_DIR/pendingQueueJ.txt echo "$latest_noZip $wresvis_noZip $systests_noZip" >> $RELEASES_DIR/pendingQueueJ.txt
    echo "System testing to be done of core revision $latest_noZip and wres-vis revision $wresvis_noZip using systests revision $systests_noZip." >> $LOGFILE 2>&1
fi

echo "=============================================================" >> $LOGFILE 2>&1
echo "" >> $LOGFILE 2>&1
