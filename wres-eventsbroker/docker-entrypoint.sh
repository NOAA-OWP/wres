#!/bin/sh

set -e

# Broker configuration path
BROKER_CONFIG_PATH=$BROKER_INSTANCE/etc/

# Properties passed to the broker and then accessible as system properties in the xml configuration files
ARTEMIS_CLUSTER_PROPS="-Dactivemq.remoting.amqp.port=${BROKER_AMQP_PORT} -Dactivemq.remoting.http.port=${BROKER_HTTP_PORT} -Dhawtio.disableProxy=true -Dhawtio.realm=activemq-cert -Dhawtio.role=wres-eventsbroker-admin -Dhawtio.offline=true -Dhawtio.sessionTimeout=86400 -Dhawtio.rolePrincipalClasses=org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal"

# Set some JVM arguments if not already set
if [[ -z $JAVA_ARGS ]]; then
    JAVA_ARGS="-XX:+PrintClassHistogram -XX:+UseG1GC -XX:+UseStringDeduplication -Xms2048m -Xmx2048m"
fi

export BROKER_CONFIG_PATH ARTEMIS_CLUSTER_PROPS JAVA_ARGS

if ! [ -f ${BROKER_CONFIG_PATH}/broker.xml ]; then
    echo "Creating broker instance in ${BROKER_INSTANCE} with arguments ${BROKER_CREATE_ARGS}"
    ${BROKER_HOME}/artemis/bin/artemis create ${BROKER_CREATE_ARGS} ${BROKER_INSTANCE}
    # Copy the broker configuration
    cp -r ${BROKER_CONFIG}/. ${BROKER_CONFIG_PATH}
    # Overwrite the logging configuration, which uses a non-default naming for the logging properties file
    # The reason for the non-default name is to avoid conflicts when running the application outside of 
    # docker and using an embedded broker, since the logging is redirected via slf4j using a 
    # logging.properties redirect
    cp ${BROKER_CONFIG_PATH}/artemis.logging.properties ${BROKER_CONFIG_PATH}/logging.properties
    rm ${BROKER_CONFIG_PATH}/artemis.logging.properties
else
    echo "Broker already created, ignoring creation."
fi

# Trap and proxy signals
trap stop TERM INT

start() {
    # Run the broker in the background and await it so that this script traps signals properly
    # https://github.com/moby/moby/issues/33319#issuecomment-457914349
    echo "Starting the wres-eventsbroker..."    
    ${BROKER_INSTANCE}/bin/artemis run &
    wait $!
}

# Stop the broker only once, regardless of how many stop signals are received
stopped="false"

stop() {
    if [ "$stopped" = "false" ]
    then
        echo "Stopping the wres-eventsbroker. Bye bye..."
        stopped="true"
        exit 0
    fi
}

start
