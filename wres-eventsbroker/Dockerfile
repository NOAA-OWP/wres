#############
# Build image
#############

FROM alpine:3.20.3 AS builder

ARG BROKER_VERSION=2.38.0

WORKDIR /workspace

# Download Artemis tgz and checksum
ADD https://dlcdn.apache.org/activemq/activemq-artemis/${BROKER_VERSION}/apache-artemis-${BROKER_VERSION}-bin.tar.gz \
    apache-artemis-${BROKER_VERSION}-bin.tar.gz
ADD https://dlcdn.apache.org/activemq/activemq-artemis/${BROKER_VERSION}/apache-artemis-${BROKER_VERSION}-bin.tar.gz.sha512 \
    apache-artemis-${BROKER_VERSION}-bin.tar.gz.sha512

# Assert checksum
RUN sha512sum -c apache-artemis-${BROKER_VERSION}-bin.tar.gz.sha512

ARG BASE_TMPDIR="out/artemis"

# Extract binaries to ${ARG BASE_TMPDIR}
RUN apk --no-cache add tar && \
    mkdir -p ${BASE_TMPDIR} && \
    tar xf apache-artemis-${BROKER_VERSION}-bin.tar.gz -C ${BASE_TMPDIR} --strip-components=1 apache-artemis-${BROKER_VERSION}/

#############
# Final image
#############

FROM registry.access.redhat.com/ubi8/ubi:8.10-1088

RUN dnf install -y \
    java-17-openjdk-headless-1:17.0.13.0.11-3.el8.x86_64 \
    procps-ng-3.3.15-14.el8 \
    iproute-6.2.0-6.el8_10.x86_64 \
    libaio-0.3.112-1.el8 \
    && dnf clean all

# Set env vars expected by BROKER
ENV BROKER_HOME=/usr/local
ENV BROKER_INSTANCE=/usr/local/wres-eventsbroker
ENV BROKER_CONFIG=/usr/local/wres-eventsbroker-config
ENV BROKER_WORK=/container_home
ENV BROKER_HTTP_PORT=15673
ENV BROKER_AMQP_PORT=5673
ENV ANONYMOUS_LOGIN=false

# Broker creation arguments. 
# The --relax-jolokia is a shorthand for the jolokia-access.xml to allow connections on all network interfaces (0.0.0.0)
ENV BROKER_CREATE_ARGS --http-host 0.0.0.0 --http-port=${BROKER_HTTP_PORT} --relax-jolokia --silent

ARG BROKER_USER=wres_eventsbroker

# Create user and group
RUN groupadd --system --gid 1370800073 wres && \
    useradd  --system --no-log-init \
             --create-home --home-dir ${BROKER_WORK} \
             --uid 502 --gid 1370800073 ${BROKER_USER}
    
WORKDIR ${BROKER_WORK}

# Create directories with the correct permissions
RUN mkdir -p {${BROKER_HOME},${BROKER_CONFIG},${BROKER_INSTANCE}} && \
    chown ${BROKER_USER} ${BROKER_INSTANCE}

COPY --from=builder /workspace/out ${BROKER_HOME}

# Copy configuration
COPY nonsrc/ ${BROKER_CONFIG}

# Use an entrypoint script
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

VOLUME /container_home
VOLUME /wres_secrets

USER ${BROKER_USER}
# Remember, to exec into a container and open a shell, use: 
# docker run -ti --entrypoint /bin/bash container_id
# Without the --entrypoint, the entrypoint script below will run
ENTRYPOINT [ "/docker-entrypoint.sh" ]

EXPOSE ${BROKER_AMQP_PORT} ${BROKER_HTTP_PORT}

HEALTHCHECK CMD curl -f localhost:${BROKER_HTTP_PORT} || exit 1

