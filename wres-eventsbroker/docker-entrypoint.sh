#!/bin/sh
# Set a lenient umask for files written by the qpid process to allow for their removal
umask 0002

# For any files not written using the default umask, make them group-writable on exit
trap stop TERM INT

start() {
    # Run qpid in background and await it so that this script traps signals properly
    # https://github.com/moby/moby/issues/33319#issuecomment-457914349
    echo "Starting the wres-eventsbroker..."    
    qpid-server --initial-config-path ${QPID_HOME}/etc/initial-config.json -prop qpid.amqp_port=5673 -prop qpid.http_port=15673 &
    wait $!
}

# Stop the broker only once, regardless of how many stop signals are received
stopped="false"

stop() {
    if [ "$stopped" = "false" ]
    then
        echo "Stopping the wres-eventsbroker. Bye bye..."
        # Make the qpid files group writable
        chmod -R 770 ${QPID_WORK}
        stopped="true"
        exit 0
    fi
}

start
