#!/bin/bash
# The top level clean-and-switch run script.

echo ""
echo ""
echo "=============================== Running At $(date)"

# The one argument is the status file.
if [ "$#" -ne 2 ]; then
    echo "Illegal number of arguments provided to cron clean and switch runner."
    echo "Two arguments are required: [config file] [status file]."
    exit 1
fi
config_file=$1
cron_clean_switch_status_file=$2

# Config file must exist!
if [ ! -f "$config_file" ]
then
    echo "The configuration file $config_file does not exist."
    echo "Aborting run."
    exit 1
fi

# If the lock file to be used exists, but is older than 6 hours, then just remove it.
if [ -f "$config_file.lock" ]
then
    # The find command will return the file name if its newer than 6 hours.
    # Otherwise, it will return nother.  Hence, the !=.  
    if [[ $(find "$config_file.lock" -newermt "6 hours ago" ) != "$config_file.lock" ]]
    then
        echo "The lock file $config_file.lock was found, but is older than 6 hours.  Removing it."
        rm -f $config_file.lock
    fi
fi

# Look for a lock using the config file's name. Trap all the possible ways
# that this process could be stopped so that the lock file doesn't hang around.
# This is pulled directly from a website. See #97182 for more info.  
if { set -C ; 2>/dev/null > $config_file.lock ; }
then
    echo "Created lock file $config_file.lock."
    trap "rm -f $config_file.lock; echo Lock file removed " EXIT TERM INT KILL 
else
    echo "Lock file $config_file.lock already exists, indicating its already running."
    echo "Aborting run." 
    exit 1
fi

# Load the configuration parameters
{ read -r cowres_url; read -r db_host1; read -r db_name1; read -r db_host2; read -r db_name2; } < <( cat $config_file )
echo "Found the following configuration options:"
echo "    cowres_url = $cowres_url"
echo "    db_host1 = $db_host1"
echo "    db_name1 = $db_name1"
echo "    db_host2 = $db_host2"
echo "    db_name2 = $db_name2"

# If the status file does not exist, initialize.
if [ ! -f "$cron_clean_switch_status_file" ]
then 
    echo "The status file $cron_clean_switch_status_file does not exist. Initializing it for a first run now."
    echo $(date)       >  $cron_clean_switch_status_file
    echo $db_name1     >>  $cron_clean_switch_status_file
    echo $db_host1     >>  $cron_clean_switch_status_file
    sleep 2 # Delay ensures the time check below leads to a run of the clean-and-switch.
fi 

# Load the parameters from the status file
{ read -r next_run_time; read -r next_db; read -r next_host; } < <( cat $cron_clean_switch_status_file )

echo "Found the following information in the status file:"    
echo "    next_run_time = $next_run_time"
echo "    next_db = $next_db"
echo "    next_host = $next_host"

# Never run if the run_state is "running".
test="running"
if [[ $run_status == "running" ]]
then
    echo "A clean is already running.  Not going to run it again."
    exit 1
fi

# Compute the Unix time stamps
next_stamp=$( date -d "${next_run_time}" +%s )
current_stamp=$( date -d "$(date)" +%s )
echo "The next run time stamp is $next_stamp.  The current time stamp is $current_stamp."
if [ $next_stamp -le $current_stamp ]
then
    echo "Next time stamp is before current time stamp.  Executing the clean and switch for $next_db."

    # Update the status file to mark it as running.
    rm $cron_clean_switch_status_file
    echo $next_run_time >   $cron_clean_switch_status_file
    echo $next_db       >>  $cron_clean_switch_status_file
    echo $next_host     >>  $cron_clean_switch_status_file

    # Execute the clean and switch.  
    ./wres_http_cleanandswitch.sh $next_db $next_host $cowres_url

    # If the error code is 0, then it succeeded.  Schedule the next run, identify the database, and 
    # update the status file.
    if [ $? -eq 0 ]
    then
        echo "Clean and switch succeeded!  Determining next run time, next db, and updating status file."

        # For next run time. Take the current date. Set the hour to an initial hour. Then increment
        # it by how often we want the clean to run, 24 hours or less, until we find a date/time in 
        # the future at least by a fixed how-far-in-the-future number of hours. That becomes the
        # next time.
        seconds_increment=43200                              # Increment specified in seconds.
        initial_date_str=$(date +"%F 00:00:00")             # Initial date/time 
        check_date_stamp=$(date -d "$initial_date_str" +%s) # Time stamp being checked based on initial date/time
        minimum_date_stamp=$(date -d "+1 hour" +%s)         # Minimum time required relative to now
        while [ $check_date_stamp -le $minimum_date_stamp ]
        do 
            check_date_stamp=$( expr $check_date_stamp + $seconds_increment )
        done

        # Go from time stamp to the next run time.
        next_run_time=$(date -ud @$check_date_stamp)

        # Be very careful to keep the name and host in sync!!! There is probably a better way to do this.
        # Decide on the next database.
        if [[ $next_db == "$db_name1" ]]
        then  
            next_db="$db_name2"
        else 
            next_db="$db_name1"
        fi
        echo "The next database name will be $next_db."

        # Decide on the next host.
        if [[ $next_host == "$db_host1" ]]
        then
            next_host="$db_host2"
        else
            next_host="$db_host1"
        fi
        echo "The next database host will be $next_host."

        # Update the status file.
        echo "Updating the status file $cron_clean_switch_status_file with $next_run_time, $next_db, $next_host..."
        rm $cron_clean_switch_status_file
        echo $next_run_time >  $cron_clean_switch_status_file
        echo $next_db       >> $cron_clean_switch_status_file
        echo $next_host     >> $cron_clean_switch_status_file
    else
        echo "Clean failed. Status file reverted to 'not running'. Try it again the next time."
        rm $cron_clean_switch_status_file
        echo $next_run_time >  $cron_clean_switch_status_file 
        echo $next_db       >> $cron_clean_switch_status_file
        echo $next_host     >> $cron_clean_switch_status_file
    fi 
else
    echo "Next time stamp is after current time stamp.  Not running the clean and switch, yet."
fi

echo "Clean-and-swith run script completed."
