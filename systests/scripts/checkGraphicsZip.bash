#!/bin/bash

if [ $# -lt 3 ]
then
        echo "Usage: $0 config_file wresZipFile LOGFILE"
        echo "config_file with a line 'user = FirstName.LastName:passwd'"
        exit 2
fi

wresZipFile=$2
CONFIGFILE=$1
LOGFILE=$3

CURL=/usr/bin/curl
PYTHON_SCRIPT_DIR=/wres_share/releases/install_scripts

echo "wres zip file = $wresZipFile" 2>&1 | /bin/tee -a $LOGFILE
sub_revision=`echo $wresZipFile | cut -d'-' -f3 | cut -d'.' -f1`
echo "sub_revision = $sub_revision" 2>&1 | /bin/tee -a $LOGFILE
wresvisZipFile=`echo $wresZipFile | sed -e s/wres\-/wres\-vis\-/`
echo "wres-vis zip file = $wresvisZipFile" 2>&1 | /bin/tee -a $LOGFILE

ARCHIVE_DIR=/wres_share/releases/archive/graphics
LATEST_ZIPFILE=

if [ ! -d $ARCHIVE_DIR ]
then
        mkdir -pv $ARCHIVE_DIR 2>&1 | /bin/tee -a $LOGFILE
fi

if [ -n "$wresvisZipFile" ]
then
	if [ -f build_recent_wres-vis.html ]
	then
		rm -v build_recent_wres-vis.html 2>&1 | /bin/tee -a $LOGFILE
	fi
	$CURL --config $CONFIGFILE --silent  https://***REMOVED***/jenkins/job/Verify_OWP_WRES/ws/wres-vis/build/distributions/ --output "build_recent_wres-vis.html"
	if [ -f build_recent_wres-vis.html ]
	then
		LATEST_ZIPFILE=`$PYTHON_SCRIPT_DIR/searchInstallBuild_wres-vis.py build_recent_wres-vis.html | grep Data | grep $sub_revision | gawk '{print ($2)}' | tr -d "')"`	
		echo "The latest wres-vis zip file = $LATEST_ZIPFILE" 2>&1 | /bin/tee -a $LOGFILE
	fi
	if [ -n "$LATEST_ZIPFILE" ]
	then
        	echo "Please wait .... currently is downloading the $wresvisZipFile file" 2>&1 | /bin/tee -a $LOGFILE
        	$CURL --config $CONFIGFILE --silent --remote-name https://***REMOVED***/jenkins/job/Verify_OWP_WRES/ws/wres-vis/build/distributions/$LATEST_ZIPFILE
		ls -l $LATEST_ZIPFILE 2>&1 | /bin/tee -a $LOGFILE
		if [ -s $LATEST_ZIPFILE ] # file exist and  size greater than 0
		then
			latest_noZip=`ls $LATEST_ZIPFILE | sed -e s/\.zip//`
			echo "latest_noZip = $latest_noZip" 2>&1 | /bin/tee -a $LOGFILE
			#echo $latest_noZip >> /wres_share/releases/pendingQueueJ_wres-vis.txt
			echo $latest_noZip > /wres_share/releases/pendingQueueJ_wres-vis.txt
			mv -v $LATEST_ZIPFILE $ARCHIVE_DIR 2>&1 | /bin/tee -a $LOGFILE
			/usr/bin/chgrp -v wres $ARCHIVE_DIR/$LATEST_ZIPFILE 2>&1 | /bin/tee -a $LOGFILE
                	/bin/chmod -v 775 $ARCHIVE_DIR/$LATEST_ZIPFILE 2>&1 | /bin/tee -a $LOGFILE
		else
			echo "There is no $wresvisZipFile." 2>&1 | /bin/tee -a $LOGFILE 
		fi
	elif [ -z "$LATEST_ZIPFILE" ]
	then
		echo "There is no $wresvisZipFile." 2>&1 | /bin/tee -a $LOGFILE 
	fi
fi
