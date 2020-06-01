FROM centos:7.8.2003

RUN yum install -y \
    java-11-openjdk-headless-11.0.7.10-4.el7_8 \
    unzip-6.0-21.el7 \
    which-2.20-7.el7 \
    # Use ss to view socket states. See #69947
    iproute-4.11.0-25.el7_7.2 \
    # The following aren't direct dependencies of WRES, are updates post-7.8.2003
    bind-license-9.11.4-16.P2.el7_8.6 \
    binutils-2.27-43.base.el7_8.1 \
    device-mapper-1.02.164-7.el7_8.2 \
    device-mapper-libs-1.02.164-7.el7_8.2 \
    systemd-219-73.el7_8.6 \
    systemd-libs-219-73.el7_8.6 \
    tzdata-2020a-1.el7 \
    yum-plugin-fastestmirror-1.1.31-54.el7_8 \
    yum-plugin-ovl-1.1.31-54.el7_8 \
    yum-utils-1.1.31-54.el7_8 \
    && yum clean all

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

