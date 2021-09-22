FROM registry.access.redhat.com/ubi8/ubi:8.4-211

RUN dnf install -y \
    java-11-openjdk-headless-1:11.0.12.0.7-0.el8_4 \
    unzip-6.0-45.el8_4 \
    procps-ng-3.3.15-6.el8 \
    iproute-5.9.0-4.el8 \
    hostname-3.20-6.el8.x86_64 \
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
EXPOSE 8443
USER wres_tasker_docker
CMD [ "bin/wres-tasker" ]

VOLUME /wres_secrets

