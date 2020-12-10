FROM centos:8.3.2011

RUN dnf install -y \
    java-11-openjdk-headless-11.0.9.11-2.el8_3 \
    unzip-6.0-43.el8 \
    which-2.21-12.el8 \
    # The following aren't direct dependencies of WRES, are updates post-8.3.2011
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

