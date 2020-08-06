FROM centos:8.2.2004

RUN dnf install -y \
    java-11-openjdk-headless-11.0.8.10-0.el8_2 \
    unzip-6.0-43.el8 \
    which-2.21-12.el8 \
    # The following aren't direct dependencies of WRES, are updates post-8.2.2004
    bind-export-libs-9.11.13-5.el8_2 \
    gnutls-3.6.8-11.el8_2 \
    libnghttp2-1.33.0-3.el8_2.1 \
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

