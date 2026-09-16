#!/bin/sh
set -e

# Properties passed to the broker and then accessible as system properties in the xml configuration files
ARTEMIS_CLUSTER_PROPS="-Dactivemq.remoting.amqp.port=${BROKER_AMQP_PORT} -Dactivemq.remoting.http.port=${BROKER_HTTP_PORT} -Dbroker.keystore.path=${BROKER_KEYSTORE_PATH} -Dbroker.keystore.password=${BROKER_KEYSTORE_PASSWORD} -Dbroker.truststore.path=${BROKER_TRUSTSTORE_PATH} -Dbroker.truststore.password=${BROKER_TRUSTSTORE_PASSWORD} -Dhawtio.disableProxy=true -Dhawtio.realm=activemq-cert -Dhawtio.roles=wres-eventsbroker-admin -Dhawtio.offline=true -Dhawtio.sessionTimeout=86400 -Dhawtio.rolePrincipalClasses=org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal"

if [[ -z $JAVA_ARGS ]]; then
    JAVA_ARGS="-XX:+PrintClassHistogram -XX:+UseG1GC -XX:+UseStringDeduplication -Xms2048m -Xmx2048m"
fi

export ARTEMIS_CLUSTER_PROPS JAVA_ARGS

# Trap and proxy signals
trap stop TERM INT

start() {
    echo "Starting the wres-eventsbroker..."    
    # Executing directly keeps the process tracking perfect
    ${BROKER_INSTANCE}/bin/artemis run &
    wait $!
}

stopped="false"
stop() {
    if [ "$stopped" = "false" ]
    then
        echo "Stopping the wres-eventsbroker. Bye bye..."
        stopped="true"
        # Optional: actively tell the background artemis process to stop gracefully if needed, 
        # though exit 0 will tear down the container.
        exit 0
    fi
}

start
