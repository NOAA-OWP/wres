#!/bin/bash

# Get the recent built zip file from Jenkins and install it
# Author: Raymond.Chui@***REMOVED***
# Created: December, 2017
#
# Arguments: First argument is a config file (any name), add a line with
#	user = firstName.lastName:passwd
#	Where the login ID and passwd are from https://***REMOVED***
#		Second argument is whether install the recent built, with value yes or no(default).
#
# Required Files: searchBuildNumber.py, searchRevisionNumber.py and searchBuiltRecent.py
# Output Files: build_history.html, build_revision.html, build_recent.html and revisionFile.txt
#		screenCapture.txt under your ~/wres_testing/wres/systests directory
# Download File: wres-yyyymmdd-subRevision.zip

if [ $# -lt 1 ]
then
	echo "Usage: $0 config_file [yes|no]"
	echo "config_file with a line 'user = FirstName.LastName:passwd'"
	exit 2
fi

TODAY=`/usr/bin/date +"%Y-%m-%d"`
LOGFILE=/wres_share/releases/install_scripts/installBuiltLog_$TODAY.txt
LOGFILE700=/wres_share/releases/install_scripts/installBuiltLog_700_$TODAY.txt
HOUR=$(date "+%_H")
MINUTE=$(date "+%_M")

if [ $HOUR -le 0 -a $MINUTE -lt 5 ]
then
        echo "HERE1"
        cat /dev/null > $LOGFILE
        cat /dev/null > $LOGFILE700
fi
echo -n "-------------------- " >> $LOGFILE
echo -n "-------------------- " >> $LOGFILE700
/usr/bin/date >> $LOGFILE
/usr/bin/date >> $LOGFILE700
echo " -----------------------" >> $LOGFILE
echo " -----------------------" >> $LOGFILE700

doSystests=no
if [ -f /wres_share/releases/systests/installing ]
then
	echo "Previous cron job is still installing" >> $LOGFILE 2>&1
	echo "Previous cron job is still installing" >> $LOGFILE700 2>&1
	doSystests=no
	exit
fi
if [ -f /wres_share/releases/install_scripts/testing.txt -o -f /wres_share/releases/install_scripts/testing700.txt ]
then
	ls -l /wres_share/releases/install_scripts/testing.txt >> $LOGFILE 2>&1
	ls -l /wres_share/releases/install_scripts/testing700.txt >> $LOGFILE700 2>&1
        echo "There is a system test running, now" >> $LOGFILE 2>&1
        echo "There is a system test running, now" >> $LOGFILE700 2>&1
        exit
fi
if [ ! -f /wres_share/releases/systests/installing ]
then
	touch /wres_share/releases/systests/installing
	ls -l /wres_share/releases/systests/installing >> $LOGFILE 2>&1
	ls -l /wres_share/releases/systests/installing >> $LOGFILE700 2>&1
fi

CONFIGFILE=$1

if [ $# -gt 1 ]
then
	INSTALL_Latest=$2
	INSTALL_Systests=$2
else
	INSTALL_Latest=no
	INSTALL_Systests=no
fi

echo "Install built = $INSTALL_Latest"
/usr/bin/date >> $LOGFILE 2>&1
/usr/bin/date >> $LOGFILE700 2>&1
CURL=/usr/bin/curl
# for now python is in ., it will change later
PYTHON_SCRIPT_DIR=/wres_share/releases/install_scripts
WORKDIR=~
# the working directory is at your home directory
cd $WORKDIR 
pwd >> $LOGFILE
pwd >> $LOGFILE700
# remvoe the old file
if [ -f build_history.html ]
then
       rm -v build_history.html >> $LOGFILE 2>&1
fi

# Step 1, get the built number from built history
#$CURL --config $CONFIGFILE --verbos  https://***REMOVED***/jenkins/job/Verify_OWP_WRES/ --output "build_history.html" 
$CURL --config $CONFIGFILE --silent  https://***REMOVED***/jenkins/job/Verify_OWP_WRES/ --output "build_history.html" 

if [ ! -s build_history.html ] # if download unsuccessful
then
       ls -l build_history.html >> $LOGFILE 2>&1
       if [ -f /wres_share/releases/systests/installing ]
	then
		rm -v /wres_share/releases/systests/installing >> $LOGFILE 2>&1
	fi
       exit 2
fi
#builtNumber=`$PYTHON_SCRIPT_DIR/searchBuildNumber.py | tail -1 | cut -d'#' -f2 | cut -d')' -f1`
builtNumber=`$PYTHON_SCRIPT_DIR/searchInstallBuild.py build_history.html | tail -1 | cut -d'#' -f2 | cut -d')' -f1`

echo "Recent built number = $builtNumber" >> $LOGFILE 2>&1

# remvoe the old file
if [ -f build_revision.html ]
then
       rm -v build_revision.html >> $LOGFILE 2>&1
fi

# Step 2, get the revision number
#$CURL --config $CONFIGFILE --verbos  https://***REMOVED***/jenkins/job/Verify_OWP_WRES/$builtNumber/ --output "build_revision.html" 
$CURL --config $CONFIGFILE --silent  https://***REMOVED***/jenkins/job/Verify_OWP_WRES/$builtNumber/ --output "build_revision.html" 

if [ ! -s build_revision.html ] # if this file is unsuccessed downlowd.
then
       ls -l build_revision.html >> $LOGFILE 2>&1
	if [ -f /wres_share/releases/systests/installing ]
        then
                rm -v /wres_share/releases/systests/installing >> $LOGFILE 2>&1
        fi
       exit 2
fi

# Step 2a, Get the sub-revision (first 6) number from revision number
#revisionNumber=`$PYTHON_SCRIPT_DIR/searchRevisionNumber.py | tail -1 | cut -d':' -f3 | tr -d " " | tr -d "')"`
revisionNumber=`$PYTHON_SCRIPT_DIR/searchInstallBuild.py build_revision.html | tail -1 | cut -d':' -f3 | tr -d " " | tr -d "')"`
echo "Revision number = $revisionNumber" | tee revisionFile.txt
sub_revision=`gawk -v revisoin=$revisionNumber '{print substr(revisoin, 0, 7)}' revisionFile.txt` 
echo "sub-revisoin number = $sub_revision" >> $LOGFILE 2>&1

# Step 3, get the built zip file
# remvoe the old file
if [ -f build_recent.html ]
then
	rm -v build_recent.html >> $LOGFILE 2>&1
fi 

LATEST_URLSITE=https://***REMOVED***/jenkins/job/Verify_OWP_WRES/ws/build/distributions/

# Get this page and output to build_recent.html
#$CURL --config $CONFIGFILE --verbos  $URLSITE --output "build_recent.html" 
$CURL --config $CONFIGFILE --silent  $LATEST_URLSITE --output "build_recent.html" 
if [ ! -s build_recent.html ] # if this file is unsuccessed downlowd.
then
	ls -l build_recent.html >> $LOGFILE 2>&1
	if [ -f /wres_share/releases/systests/installing ]
        then
                rm -v /wres_share/releases/systests/installing >> $LOGFILE 2>&1
        fi
	exit 2
fi
if [ -f systests_recent.html ]
then
	rm -v systests_recent.html >> $LOGFILE 2>&1
fi
SYSTESTS_URLSITE=https://***REMOVED***/jenkins/job/Verify_OWP_WRES/ws/systests/build/distributions/
$CURL --config $CONFIGFILE --silent  $SYSTESTS_URLSITE --output "systests_recent.html" 
if [ ! -s systests_recent.html ] # if download unsuccessful
then
	ls -l systests_recent.html >> $LOGFILE 2>&1
	if [ -f /wres_share/releases/systests/installing ]
        then
                rm -v /wres_share/releases/systests/installing >> $LOGFILE 2>&1
        fi
	exit 2
fi
# search he recent built file name
LATEST_ZIPFILE=`$PYTHON_SCRIPT_DIR/searchInstallBuild.py build_recent.html | grep Data | grep $sub_revision | gawk '{print ($2)}' | tr -d "')"`
SYSTESTS_ZIPFILE=`$PYTHON_SCRIPT_DIR/searchInstallBuild.py systests_recent.html | grep Data | grep $sub_revision | gawk '{print ($2)}' | tr -d "')"`

echo "Recent built zip file = $LATEST_ZIPFILE" >> $LOGFILE 2>&1
echo "Recent systests zip file = $SYSTESTS_ZIPFILE" >> $LOGFILE 2>&1
# Note there is a bug here, sometimes when the sub_revision number not match between latest zip file and systest zip file, then sys tests will broken.
# Fixed this bug at August 7th, 2018 by RHC

if [ -z "$SYSTESTS_ZIPFILE" ]
then
	echo "The systest zip file doesn't match the latest zip file." >> $LOGFILE 2>&1
	cp -pv systests_recent.html systests_recentAll.html >> $LOGFILE 2>&1
	$PYTHON_SCRIPT_DIR/searchInstallBuild.py systests_recentAll.html | grep Data | gawk -F "'" '{print($4,$8)}' | tr -d "," | gawk '{print($4,$2,$3,$5,$6,$1)}' > unsortrecentsystest.txt
	SYSTESTS_ZIPFILE=`$PYTHON_SCRIPT_DIR/formatLine.py unsortrecentsystest.txt | grep -v Invalid | sort | tail -1 | cut -d' ' -f2`
fi

#Get the name of the zip file without the .zip extension.
# see whether the built has installed?
if [ -n "$LATEST_ZIPFILE" ]
then
	unZip_Latest=`echo $LATEST_ZIPFILE | tr -d ".zip"`
	if [ -d /wres_share/releases/$unZip_Latest ]
	then
        	ls -d /wres_share/releases/$unZip_Latest >> $LOGFILE 2>&1
        	echo " already installed" >> $LOGFILE 2>&1
		INSTALL_Latest=no
	fi
elif [ -z "$LATEST_ZIPFILE" ]
then
	INSTALL_Latest=no
fi

if [ -n "$SYSTESTS_ZIPFILE" ]
then
	unZip_SysTests=`echo $SYSTESTS_ZIPFILE | tr -d ".zip"`
	if [ -d /wres_share/releases/$unZip_SysTests ]
	then 
		ls -d /wres_share/releases/$unZip_SysTests >> $LOGFILE 2>&1
		echo " already installed" >> $LOGFILE 2>&1
		INSTALL_Systests=no
	fi
elif [ -z "$SYSTESTS_ZIPFILE" ]
then
	INSTALL_Systests=no	
fi

ARCHIVE_DIR=/wres_share/releases/archive
if [ ! -d $ARCHIVE_DIR ]
then
	mkdir -pv $ARCHIVE_DIR >> $LOGFILE 2>&1
fi

# Step 4, ready to download
# Get the recent built zip file and recent systests zip file
#$CURL --config $CONFIGFILE --verbos --remote-name $URLSITE$ZIPFILE 
if [ "$INSTALL_Latest" = "yes" ]
then
	echo "Please wait .... currently is downloading the $LATEST_ZIPFILE file" >> $LOGFILE 2>&1
	$CURL --config $CONFIGFILE --silent --remote-name $LATEST_URLSITE$LATEST_ZIPFILE 
fi
if [ "$INSTALL_Systests" = "yes" ]
then
	echo "Please wait .... currently is downloading the $SYSTESTS_ZIPFILE file" >> $LOGFILE 2>&1
	$CURL --config $CONFIGFILE --silent --remote-name $SYSTESTS_URLSITE$SYSTESTS_ZIPFILE 
fi
# Step 4a, ready to install the latest built
if [ -n "$LATEST_ZIPFILE" -a -f $LATEST_ZIPFILE ] # if the recent built zip file has downloaded
then
	ls -l $LATEST_ZIPFILE >> $LOGFILE 2>&1
	latest_noZip=`ls $LATEST_ZIPFILE | tr -d ".zip"`
	echo "install built file is $noZip" >> $LOGFILE 2>&1
	if [ "$INSTALL_Latest" = "yes" ]
	then
		/wres_share/releases/install_scripts/installrls.fromzip.sh $latest_noZip >> $LOGFILE 2>&1
		mv -v $LATEST_ZIPFILE $ARCHIVE_DIR >> $LOGFILE 2>&1
		/usr/bin/chgrp -v wres $ARCHIVE_DIR/$LATEST_ZIPFILE >> $LOGFILE 2>&1
		chmod 775 $ARCHIVE_DIR/$LATEST_ZIPFILE >> $LOGFILE 2>&1
		echo "$builtNumber # $latest_noZip" >> $ARCHIVE_DIR/recordDownloadedNBuilt.txt
		doSystests=yes
		# TODO
		# Check wres-vis graphics zip file
		/wres_share/releases/install_scripts/checkGraphicsZip.bash ~/mytoken $LATEST_ZIPFILE $LOGFILE
	fi
fi

SYSTESTS_ARCHIVE_DIR=/wres_share/releases/systests_archive
if [ ! -d $SYSTESTS_ARCHIVE_DIR ]
then
        mkdir -pv $SYSTESTS_ARCHIVE >> $LOGFILE 2>&1
fi

# Step 4b, ready to intall the systests
if [ -n "$SYSTESTS_ZIPFILE" ]
then
	if [ -f $SYSTESTS_ZIPFILE ] # if the recent systests zip file has downloaded
	then
		ls -l $SYSTESTS_ZIPFILE >> $LOGFILE 2>&1
		systests_noZip=`ls $SYSTESTS_ZIPFILE | tr -d ".zip"`
		echo "INSTALL_Systests = $INSTALL_Systests " >> $LOGFILE 2>&1
		if [ "$INSTALL_Systests" = "yes" ]
        	then
			/wres_share/releases/install_scripts/installsystests.fromzip.sh $systests_noZip >> $LOGFILE 2>&1
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

# Now do the systests if the recent built has installed
#INSTALL_Latest=no
#INSTALL_Systests=no
echo "INSTALL_Latest = $INSTALL_Latest, INSTALL_Systests = $INSTALL_Systests" >> $LOGFILE 2>&1
if [ "$INSTALL_Latest" = "yes" -o "$INSTALL_Systests" = "yes" ]
then
	doSystests=yes
fi
echo "doSystests =  $doSystests" >> $LOGFILE 2>&1
#exit
if [ "$doSystests" = "yes" ]
then
	# append the latest revision number into the queue
#	echo $latest_noZip >> /wres_share/releases/pendingQueue.txt
	echo $latest_noZip >> /wres_share/releases/pendingQueueJ.txt
# -------------- Started at 03/06/2019, the system tests and emailing results will in differenct process in cron. ------------------
#	if [ -L /wres_share/releases/systests ] # if it is a symbolic link
#	then
#		cd /wres_share/releases/systests
#		pwd >> $LOGFILE 2>&1
#		#if [ -x rundevtests.sh ] # if this file is exists and is executable
#		if [ -x runsystests.sh ] # if this file is exists and is executable
#		then
#			export WRES_DB_NAME=wres4
#			export WRES_LOG_LEVEL=info
#			echo "WRES DB = $WRES_DB_NAME" >> $LOGFILE 2>&1
#			echo "WRES Log level = $WRES_LOG_LEVEL" >> $LOGFILE 2>&1
#			# test 900 only in nohup background
#			/usr/bin/nohup ./runtest_db900.sh -l -d wres5 -t $PWD -r $latest_noZip scenario9* | tee systests_900screenCatch_$latest_noZip.txt &
#			# test all except 700 and 900
#			./runsystests.sh -l 2>&1 | tee systests_screenCatch_$latest_noZip.txt
#			 egrep -C 3 '(diff|FAIL)' systests_screenCatch_$latest_noZip.txt > systests_Results_$latest_noZip.txt
#			grep "Aborting all tests" /wres_share/releases/install_scripts/installBuiltLog.txt >> systests_Results_$latest_noZip.txt
#			# test 700 only
#			./runsystests_700.sh -l 2>&1 | tee systests_screenCatch_700_$latest_noZip.txt
#			 egrep -C 3 '(diff|FAIL)' systests_screenCatch_700_$latest_noZip.txt > systests_Results_700_$latest_noZip.txt
#			#cleandatabase=`grep "Aborting all tests" /wres_share/releases/install_scripts/installBuiltLog.txt`
#			#grep "Aborting all tests" /wres_share/releases/install_scripts/installBuiltLog.txt >> systests_Results_700_$latest_noZip.txt
#			
#
#			# Now, let's email them out
#			if [ -s systests_Results_$latest_noZip.txt ] # there are diff and/or FAIL found in this test
#			then # file exists and size greater than 0
#				# zip the latest wresSysTestResults_*.txt file
#				today=$(date +%Y%m%d)
#				wresSysTestResults=`ls -t wresSysTestResults_"$today"*.txt | head -1`
#				/usr/bin/zip "$wresSysTestResults".zip $wresSysTestResults >> $LOGFILE 2>&1
#
#				/usr/bin/mailx -F -S smtp=140.90.91.135 -s "systests results from $latest_noZip FAILED" -a "$wresSysTestResults".zip Raymond.Chui@***REMOVED***,Hank.Herr@***REMOVED***,james.d.brown@***REMOVED***,jesse.bickel@***REMOVED***,christopher.tubbs@***REMOVED*** < systests_screenCatch_$latest_noZip.txt 
#				echo "Email out $wresSysTestResults.zip and systests_screenCatch_$latest_noZip.txt" >> $LOGFILE 2>&1
#			elif [ ! -s systests_Results_$latest_noZip.txt ] # there are no diff nor FAIL found in this test
#			then # file is not exist or empty size
#				echo "No diff, FAILS, FATAL, ERROR, Abort  found in this test." >> $LOGFILE 2>&1
#				grep -A2 PROCESSING systests_screenCatch_$latest_noZip.txt > systests_Results_$latest_noZip.txt
#				/usr/bin/mailx -F -S smtp=140.90.91.135 -s "systests results from $latest_noZip PASSED" Raymond.Chui@***REMOVED***,Hank.Herr@***REMOVED***,james.d.brown@***REMOVED***,jesse.bickel@***REMOVED***,christopher.tubbs@***REMOVED*** < systests_Results_$latest_noZip.txt
#				rm -v systests_Results_$latest_noZip.txt >> $LOGFILE 2>&1
#			fi
#
#			if [ -s systests_Results_700_$latest_noZip.txt ] # there are diff and/or FAIL found in this test
#                        then # file exists and size greater than 0
#                                # zip the latest wresSysTestResults_*.txt file
#                                today=$(date +%Y%m%d)
#                                wresSysTestResults_700=`ls -t wresSysTestResults_700_"$today"*.txt | head -1`
#                                /usr/bin/zip "$wresSysTestResults_700".zip $wresSysTestResults_700 >> $LOGFILE700 2>&1
#
#                                /usr/bin/mailx -F -S smtp=140.90.91.135 -s "systests results from $latest_noZip for 700 FAILED" -a "$wresSysTestResults_700".zip Raymond.Chui@***REMOVED***,Hank.Herr@***REMOVED***,james.d.brown@***REMOVED***,jesse.bickel@***REMOVED***,christopher.tubbs@***REMOVED*** < systests_screenCatch_700_$latest_noZip.txt
#                                echo "Email out $wresSysTestResults_700.zip and systests_screenCatch_700_$latest_noZip.txt" >> $LOGFILE700 2>&1
#                        elif [ ! -s systests_Results_700_$latest_noZip.txt ] # there are no diff nor FAIL found in this test
#                        then # file is not exist or empty size
#                                echo "No diff, FAILS, FATAL, ERROR, Abort  found in this test." >> $LOGFILE700 2>&1
#				grep -A2 PROCESSING systests_screenCatch_700_$latest_noZip.txt > systests_Results_700_$latest_noZip.txt
#				/usr/bin/mailx -F -S smtp=140.90.91.135 -s "systests results from $latest_noZip for 700 PASSED" Raymond.Chui@***REMOVED***,Hank.Herr@***REMOVED***,james.d.brown@***REMOVED***,jesse.bickel@***REMOVED***,christopher.tubbs@***REMOVED*** < systests_Results_700_$latest_noZip.txt
#                                rm -v systests_Results_700_$latest_noZip.txt >> $LOGFILE700 2>&1
#                        fi
#
#			./archiveResults.sh $latest_noZip
#
#			#curl smtp://mail.example.com --mail-from myself@example.com --mail-rcpt receiver@example.com --upload-file email.txt
#			# $CURL --config $CONFIGFILE smtp://smtp.gmail.com  --mail-from Raymond.Chui@***REMOVED*** --mail-rcpt Raymond.Chui@***REMOVED*** --upload-file systests_screenCatch_$latest_noZip.txt
#			touch /wres_share/releases/$latest_noZip/tested.txt
#		fi
#	fi
fi

if [ -f /wres_share/releases/systests/installing ]
then
	rm -v /wres_share/releases/systests/installing >> $LOGFILE 2>&1
fi
echo "====================================================================================================================" >> $LOGFILE 2>&1
echo "" >> $LOGFILE 2>&1
#cat /wres_share/releases/install_scripts/installBuiltLog.txt >> /wres_share/releases/install_scripts/installBuiltLog_$TODAY.txt
#/usr/bin/find -P /wres_share/releases/install_scripts -maxdepth 1 -name "installBuiltLog_*" -mtime +1 -exec rm -v {}\; >> $LOGFILE 2>&1 
/usr/bin/find -P /wres_share/releases/install_scripts -maxdepth 1 -name "installBuiltLog_*" -mtime +1 -exec rm -v {} \; >> $LOGFILE 2>&1 
