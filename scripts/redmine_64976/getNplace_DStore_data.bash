#!/bin/bash

# Description:	This BASH script is to get the data files from D-store site by a specify date (yyyymmdd), default is today,
#				and store them into a remote host directory by an user's ID with the SSH keys	
#				See README_1st.txt for how to create those keys
#
# Author:	Raymond.Chui@***REMOVED***
# Created:	June, 2019	
# To Call:	searchTheIndex.py Python script within the same directory 
# Called By: getMultipleDataTypes.bash
# Log File:	nc_logs.txt

TODAY=`date -u "+%Y%m%d"` # get date in GMT time

WHATDATE=$TODAY # use -w option to override this
DStore_Site="http://***REMOVED***dstore.***REMOVED***.***REMOVED***" # for now this is fixed
DATAVERSION=2.0
DATATYPE= # this is requiured by -t option
DESTINATION_DIR=/mnt/wres_share/nwmData # use -d option to override this 
DESTINATION_HOST=***REMOVED***wres-dev02.***REMOVED***.***REMOVED*** # use -h option to override this 
REMOTE_USER=$USER # use -u option to override this 
CURRENTDIR=$PWD
SCRIPT_DIR=$PWD # use -s option to override this 
LOGFILE=$SCRIPT_DIR/nc_logs.txt
SSHKEYS="YES"

while getopts "h:u:d:w:s:V:t:k:l:" opt; do
	case $opt in
		h)
			DESTINATION_HOST=$OPTARG
			;;
		d)
			DESTINATION_DIR=$OPTARG
			;;
		u)
			REMOTE_USER=$OPTARG
			;;
		w)
			WHATDATE=$OPTARG
			;;
		s)
			SCRIPT_DIR=$OPTARG
			;;
		V)
			DATAVERSION=$OPTARG
			;;
		t)
			DATATYPE=$OPTARG
			;;
		k)
			SSHKEYS=$OPTARG
			;;
		l)
			LOGFILE=$OPTARG
			;;
		\?)
			echo "Usage: $0 -t data_type [-h destination_host -d /destination_dir -u remote_loginID -w yyyymmdd -s thisScript_dir -V data_version -l log_file -k YES/NO]"
			exit 2
			;;
	esac
done

if [ -z "$DATATYPE" ]
then
	echo "Usage: $0 -t data_type [-h destination_host -d /destination_dir -u remote_loginID -w yyyymmdd -s thisScript_dir -V data_version -l log_file -k YES/NO]"
	exit 2
fi

DStore_Dir="nwm/$DATAVERSION/nwm.$WHATDATE" # presumable

#umask 002 # set 664,this only set local host, don't know how to setin remote host, yet.
 
cd $SCRIPT_DIR

# get the D-Store data directory and save it as yyyymmdd_index.html file
/usr/bin/curl --silent "$DStore_Site"/"$DStore_Dir"/ --output "$WHATDATE"_index.html
if [ ! -f "$WHATDATE"_index.html ]
then
	echo "ERROR: Couldn't get directory from $DStore_Site/$DStore_Dir/ and saved to $WHATDATE _index.html" | tee --append $LOGFILE
	exit 2
fi

# list the range directories
range_dirs=`$SCRIPT_DIR/searchTheIndex.py "$WHATDATE"_index.html "$DATATYPE" "$WHATDATE"`
rm -v "$WHATDATE"_index.html | tee --append $LOGFILE 2>&1

for range_dir in $range_dirs
do
	echo "Start $range_dir at `date`" | tee --append $LOGFILE
	# create the directory paths in target host
	if [ "$SSHKEYS" = "YES" ]
	then
		ssh -q $REMOTE_USER@$DESTINATION_HOST mkdir -pv "$DESTINATION_DIR"/"$DStore_Dir"/"$range_dir" 2> /dev/null | tee --append $LOGFILE
	elif [ "$SSHKEYS" = "NO" ]
	then
		mkdir -pv --mode=664 "$DESTINATION_DIR"/"$DStore_Dir"/"$range_dir" 2> /dev/null | tee --append $LOGFILE
	fi

	# get the list of files in each range directory and save in a *_index.html file
	/usr/bin/curl --silent "$DStore_Site"/"$DStore_Dir"/"$range_dir"/  --output "$range_dir"_index.html
	if [ ! -f "$range_dir"_index.html ]
	then
		echo "ERROR: Couldn't get directory from $DStore_Site/$DStore_Dir/$range_dir/ and saved to $range_dir _index.html" | tee --append $LOGFILE
		exit 2
	fi

	# search each file URL in D-Store
	rangeFiles=`$SCRIPT_DIR/searchTheIndex.py "$range_dir"_index.html "$DATATYPE" "$WHATDATE"`
	fileCount=0
	for rangeFile in $rangeFiles
	do
		# check the file size in source (D-Store)
		dStoreFileSize=`/usr/bin/curl -s --head $DStore_Site/$DStore_Dir/$range_dir/$rangeFile | gawk '/^Content-Length/ { printf("%d", $2)}'`

		# get each file
		if [ "$SSHKEYS" = "YES" ]
		then
			ssh -q $REMOTE_USER@$DESTINATION_HOST test -f $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile 2> /dev/null
			if [ $? -eq 0 ] # if test -f file exists (true), the return status $? is 0
			then
				#echo "$? --> INFO: $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile is already exitsted"  | tee --append $LOGFILE 2>&1
				remoteFileSize=`ssh -q $REMOTE_USER@$DESTINATION_HOST ls -l $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile 2> /dev/null | gawk '{printf("%d", $5)}'`
				if [ -z "$remoteFileSize" ] # for some reason unable to get its file size
				then
					echo "WARN: target file existed $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile, but its size is empty" | tee --append $LOGFILE
					# remove the corrupted target file and re-download it
					ssh -q $REMOTE_USER@$DESTINATION_HOST rm -v $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile 2> /dev/null | tee --append $LOGFILE
					ssh -q $REMOTE_USER@$DESTINATION_HOST /usr/bin/curl $DStore_Site/$DStore_Dir/$range_dir/$rangeFile --silent --show-error --connect-timeout 10 --max-time 30 --retry 2 --retry-delay 1 --retry-max-time 2 --data-binary --remote-time --output $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile 2> /dev/null
					fileCount=`expr $fileCount + 1`
				elif [ -n "$remoteFileSize" ] # make sure the file size isn't an empty string
				then
					if [ $dStoreFileSize -ne $remoteFileSize ] # compare the file sizes with digital number
					then
						echo "WARN: target file $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile size is $remoteFileSize which doesn't match DStore file size $dStoreFileSize"  | tee --append $LOGFILE
						# remove the corrupted target file and re-download it
						ssh -q $REMOTE_USER@$DESTINATION_HOST rm -v $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile 2> /dev/null | tee --append $LOGFILE
						ssh -q $REMOTE_USER@$DESTINATION_HOST /usr/bin/curl $DStore_Site/$DStore_Dir/$range_dir/$rangeFile --silent --show-error --connect-timeout 10 --max-time 30 --retry 2 --retry-delay 1 --retry-max-time 2 --data-binary --remote-time --output $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile 2> /dev/null
						fileCount=`expr $fileCount + 1`
					else
						continue
					fi
				fi
			else # Otherwise, return status $? is a non-zero, that means file isn't existed, need to download
				ssh -q $REMOTE_USER@$DESTINATION_HOST /usr/bin/curl $DStore_Site/$DStore_Dir/$range_dir/$rangeFile --silent --show-error --connect-timeout 10 --max-time 30 --retry 2 --retry-delay 1 --retry-max-time 2 --data-binary --remote-time --output $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile 2> /dev/null

				# check whether the target file success download to target directory 
				ssh -q $REMOTE_USER@$DESTINATION_HOST test -f $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile 2> /dev/null
				if [ $? -eq 0 ] # if download successed
				then
					# check the target file size, remote execute file size by word count bytes (wc -c)
					remoteFileSize=`ssh -q $REMOTE_USER@$DESTINATION_HOST wc -c $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile 2> /dev/null | gawk '{printf("%d", $1)}'`
				fi
				if [ -z "$remoteFileSize" ] # unable to detected the target file size
				then
					echo "WARN: unable detected $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile file size." | tee --append $LOGFILE
				elif [ -n "$remoteFileSize" ] # make sure the file size isn't an empty string
				then
					if [ $dStoreFileSize -ne $remoteFileSize ] # compare the file sizes with digital number
					then
						echo "WARN: $DStore_Dir/$range_dir/$rangeFile is $dStoreFileSize, and $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile is $remoteFileSize" | tee --append $LOGFILE
						ssh -q $REMOTE_USER@$DESTINATION_HOST rm -v $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile 2> /dev/null | tee --append $LOGFILE
					else
						fileCount=`expr $fileCount + 1`
					fi
				fi
			fi
		elif [ "$SSHKEYS" = "NO" ]
		then
			echo "Please setup your SSH keys first before you execute this script." | tee --append $LOGFILE
			#/usr/bin/curl $DStore_Site/$DStore_Dir/$range_dir/$rangeFile --silent --show-error --connect-timeout 10 --max-time 30 --retry 2 --retry-delay 1 --retry-max-time 2 --data-binary --remote-time --output $DESTINATION_DIR/$DStore_Dir/$range_dir/$rangeFile
		fi
	done # ends the inner for loop
	rm -v "$range_dir"_index.html | tee --append $LOGFILE
	echo "End $range_dir at `date`, total file downloaded: $fileCount" | tee --append $LOGFILE
done # ends the outer for loop

ssh -q $REMOTE_USER@$DESTINATION_HOST chmod -R g+w $DESTINATION_DIR 2> /dev/null
