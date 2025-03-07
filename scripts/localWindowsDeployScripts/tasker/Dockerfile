FROM registry.access.redhat.com/ubi8/ubi:8.8-1009

RUN dnf install -y \
    java-17-openjdk-headless-1:17.0.13.0.11-3.el8.x86_64 \
    unzip-6.0-46.el8 \
    procps-ng-3.3.15-14.el8 \
    iproute-6.2.0-5.el8_9 \
    hostname-3.20-6.el8 \
    && dnf clean all

# See comments in main/worker/root wres Dockerfile
RUN groupadd --gid 1370800073 wres \
    && useradd --uid 499 --gid 1370800073 wres_tasker_docker

ARG version
WORKDIR /opt
COPY ../../../wres-tasker/build/distributions/wres-tasker-${version}.zip .
RUN unzip wres-tasker-${version}.zip \
    && rm wres-tasker-${version}.zip

WORKDIR /opt/wres-tasker-${version}

# Use an entrypoint script
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

# Concert windows line ending to UNIX
RUN dnf install -y dos2unix
RUN dos2unix /docker-entrypoint.sh

EXPOSE 8443
USER wres_tasker_docker

# Remember, to exec into a container and open a shell, use: 
# docker run -ti --entrypoint /bin/bash container_id
# Without the --entrypoint, the entrypoint script below will run
ENTRYPOINT [ "/docker-entrypoint.sh" ]

VOLUME /wres_secrets

