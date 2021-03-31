# Based on: https://hub.docker.com/r/chrisob/qpid-broker-j-docker/dockerfile

#############
# Build image
FROM alpine AS builder

ARG BROKER_J_VERSION=8.0.4

WORKDIR /workspace

# Download Broker-J tgz and checksum
ADD https://archive.apache.org/dist/qpid/broker-j/${BROKER_J_VERSION}/binaries/apache-qpid-broker-j-${BROKER_J_VERSION}-bin.tar.gz \
    apache-qpid-broker-j-${BROKER_J_VERSION}-bin.tar.gz
ADD https://archive.apache.org/dist/qpid/broker-j/${BROKER_J_VERSION}/binaries/apache-qpid-broker-j-${BROKER_J_VERSION}-bin.tar.gz.sha512 \
    apache-qpid-broker-j-${BROKER_J_VERSION}-bin.tar.gz.sha512

# Assert checksum
RUN sha512sum -c apache-qpid-broker-j-${BROKER_J_VERSION}-bin.tar.gz.sha512

# Extract binaries to ./out/
RUN apk add tar && \ 
    mkdir out && \
    tar xf apache-qpid-broker-j-${BROKER_J_VERSION}-bin.tar.gz -C out --strip-components=2 qpid-broker/${BROKER_J_VERSION}/

#############
# Final image
FROM azul/zulu-openjdk-debian:11.0.10-11.45.27

# Set env vars expected by qpid
ENV QPID_HOME=/usr/local
ENV QPID_WORK=/container_home
ENV QPID_RUN_LOG=2
ENV QPID_HTTP_PORT=15673
ENV QPID_AMQP_PORT=5673

# Create user and group
RUN groupadd --system --gid 1370800073 wres && \
    useradd  --system --no-log-init \
             --create-home --home-dir ${QPID_WORK} \
             --uid 502 --gid 1370800073 wres_eventsbroker

WORKDIR ${QPID_WORK}

# Copy qpid binaries from build stage to QPID_HOME
RUN mkdir -p ${QPID_HOME}
COPY --from=builder /workspace/out ${QPID_HOME}

# Copy initial-config.json
COPY nonsrc/initial-config.json ${QPID_HOME}/etc/

# In order to set umask for file sharing, use a wrapper script
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

VOLUME /container_home
VOLUME /wres_secrets

USER wres_eventsbroker
ENTRYPOINT [ "/docker-entrypoint.sh" ]

EXPOSE ${QPID_AMQP_PORT} ${QPID_HTTP_PORT}
