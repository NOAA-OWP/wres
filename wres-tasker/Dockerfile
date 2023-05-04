FROM registry.access.redhat.com/ubi8/ubi:8.7-1112

RUN dnf install -y \
    java-17-openjdk-headless-1:17.0.7.0.7-1.el8_7 \
    unzip-6.0-46.el8 \
    procps-ng-3.3.15-9.el8 \
    iproute-5.18.0-1.el8 \
    hostname-3.20-6.el8 \
    && dnf clean all

# See comments in main/worker/root wres Dockerfile
RUN groupadd --gid 1370800073 wres \
    && useradd --uid 499 --gid 1370800073 wres_tasker_docker

ARG version
WORKDIR /opt
COPY ./build/distributions/wres-tasker-${version}.zip .
RUN unzip wres-tasker-${version}.zip \
    && rm wres-tasker-${version}.zip

WORKDIR /opt/wres-tasker-${version}

# Use an entrypoint script
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8443
USER wres_tasker_docker

# Remember, to exec into a container and open a shell, use: 
# docker run -ti --entrypoint /bin/bash container_id
# Without the --entrypoint, the entrypoint script below will run
ENTRYPOINT [ "/docker-entrypoint.sh" ]

VOLUME /wres_secrets

