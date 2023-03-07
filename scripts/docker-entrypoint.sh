#!/bin/sh

set -e

# In order for worker-created files to be deleted by tasker, need group write,
# and in order for the java process to successfully set group write permissions,
# need more lenient umask.
umask 0002

set -e

# For gradle >= v6.9.2, the application start script will not perform command substitution or resolve environment
# variables at container runtime, so that must be done here.
CON_HOSTNAME=`hostname`

# Replace the environment variables with the shell variables of the same name
MODIFIED_OPTS=$(echo $JAVA_OPTS | sed 's/$CON_HOSTNAME/'"$CON_HOSTNAME"'/g')
MODIFIED_INNER_OPTS=$(echo $INNER_JAVA_OPTS | sed 's/$CON_HOSTNAME/'"$CON_HOSTNAME"'/g')

# Export them
export JAVA_OPTS=$MODIFIED_OPTS
export INNER_JAVA_OPTS=$MODIFIED_INNER_OPTS

# Trap and proxy signals
trap stop TERM INT

start() {
    # Run the client in the background and await it so that this script traps signals properly
    # https://github.com/moby/moby/issues/33319#issuecomment-457914349
    echo "Starting the wres-worker client..."    
    ./bin/wres-worker /usr/bin/wres &
    wait $!
}

# Stop the client only once, regardless of how many stop signals are received
stopped="false"

stop() {
    if [ "$stopped" = "false" ]
    then
        echo "Stopping the wres-worker client. Bye bye..."
        stopped="true"
        exit 0
    fi
}

start
