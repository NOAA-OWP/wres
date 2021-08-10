#!/bin/bash

# Description:	This BASH script is to get the multiple data file types from D-store site by a specify date (yyyymmdd), default is today,
#				and store them into a remote host directory by an user's ID with the SSH keys	
#				See README_1st.txt for how to create those keys
#
# Author:	Raymond.Chui@***REMOVED***
# Created:	June, 2019	
# To Call:	getNplace_DStore_data.bash BASH script within the same directory 
# Log File:	nc_logs.txt

TODAY=`date -u "+%Y%m%d"` # get date in GMT time

WHATDATES=$TODAY # use -w option to override this
DStore_Site=$DSTORE_URL
DATAVERSION=2.0
DATATYPES= # this is requiured by -t option
DESTINATION_DIR=/mnt/wres_share/nwmData # use -d option to override this 
DESTINATION_HOST=localhost # use -h option to override this
REMOTE_USER=$USER # use -u option to override this 
CURRENTDIR=$PWD
SCRIPT_DIR=$PWD # use -s option to override this 
LOGFILE=$SCRIPT_DIR/nc_logs.txt
SSHKEYS="yes"

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
			WHATDATES=$OPTARG
			;;
		s)
			SCRIPT_DIR=$OPTARG
			;;
		V)
			DATAVERSION=$OPTARG
			;;
		t)
			DATATYPES=$OPTARG
			;;
		k)
			SSHKEYS=$OPTARG
			;;
		l)
			LOGFILE=$OPTARG
			;;
		\?)
			echo "Usage: $0 -t data_types [-h destination_host -d /destination_dir -u remote_loginID -w yyyymmdd [yyyymmdd2] -s thisScript_dir -V data_version -l log_file -k YES/NO]"
			exit 2
			;;
	esac
done

if [ -z "$DATATYPES" ]
then
	echo "Usage: $0 -t data_types [-h destination_host -d /destination_dir -u remote_loginID -w yyyymmdd [yyyymmdd2] -s thisScript_dir -V data_version -l log_file -k YES/NO]"
	exit 2
fi

SSHKEYS=`echo $SSHKEYS | tr [a-z] [A-z]` # enforce to all upper case
echo "ssh key = $SSHKEYS"
if [ "$SSHKEYS" = "NO" ]
then
	echo "Please setup your SSH keys first before you execute this script."
	exit 2
fi

cd $SCRIPT_DIR
if [ -f $LOGFILE ]
then
	rm -v $LOGFILE # remove the old log file
fi

echo " -w option $WHATDATES" | tee --append $LOGFILE
TODATE=`echo $WHATDATES | gawk '{print($NF)}'`
FROMDATE=`echo $WHATDATES | gawk '{print($1)}'`
if [ $FROMDATE -gt $TODATE ]
then
	echo "-w $FROMDATE must less or equals then $TODATE"
	exit 2
fi

echo "get files from $FROMDATE and $TODATE" | tee --append $LOGFILE

while [ $FROMDATE -le $TODATE ] # get the data files from yyyymmdd1 to yyyymmdd2
do
	WHATDATE=$FROMDATE
	echo "get files for $WHATDATE" | tee --append $LOGFILE 
	DStore_Dir="nwm/$DATAVERSION/nwm.$WHATDATE" # presumable
	for DATATYPE in $DATATYPES # get data type1, type2, ... typeN
	do
		echo "get data type $DATATYPE" | tee --append $LOGFILE 
		./getNplace_DStore_data.bash -t $DATATYPE -h $DESTINATION_HOST -d $DESTINATION_DIR -u $REMOTE_USER -s $SCRIPT_DIR -V $DATAVERSION -w $WHATDATE -l $LOGFILE -k $SSHKEYS 
	done
	FROMDATE=`expr $FROMDATE + 1` # increment by 1 day
	echo "Increment date to $FROMDATE" | tee --append $LOGFILE
done
