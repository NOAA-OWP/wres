#!/bin/sh

set -e

# For gradle >= v6.9.2, the application start script will not perform command substitution or resolve environment
# variables at container runtime, so that must be done here.
CON_HOSTNAME=`hostname`

# Replace the environment variables with the shell variables of the same name
MODIFIED_OPTS=$(echo $JAVA_OPTS | sed 's/$CON_HOSTNAME/'"$CON_HOSTNAME"'/g' | sed 's/$JFR_FILENAME/'"$JFR_FILENAME"'/g')

# Export them
export JAVA_OPTS=$MODIFIED_OPTS

# Trap and proxy signals
trap stop TERM INT

start() {
    # Run the client in the background and await it so that this script traps signals properly
    # https://github.com/moby/moby/issues/33319#issuecomment-457914349
    echo "Starting the wres-tasker..."    
    ./bin/wres-tasker &
    wait $!
}

# Stop the client only once, regardless of how many stop signals are received
stopped="false"

stop() {
    if [ "$stopped" = "false" ]
    then
        echo "Stopping the wres-tasker. Bye bye..."
        stopped="true"
        exit 0
    fi
}

start