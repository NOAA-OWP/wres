FROM registry.access.redhat.com/ubi8/ubi:8.7-1037

RUN dnf install -y \
    java-11-openjdk-headless-1:11.0.17.0.8-2.el8_6 \
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
EXPOSE 8443
USER wres_tasker_docker
CMD [ "bin/wres-tasker" ]

VOLUME /wres_secrets

