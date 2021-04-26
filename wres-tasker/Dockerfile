FROM registry.access.redhat.com/ubi8/ubi:8.3-297.1618432833

RUN dnf install -y \
    java-11-openjdk-headless-11.0.11.0.9-0.el8_3 \
    unzip-6.0-43.el8 \
    procps-ng-3.3.15-3.el8_3.1 \
    iproute-5.3.0-5.el8 \
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

