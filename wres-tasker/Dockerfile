FROM centos:7.7.1908

RUN yum install -y \
    java-11-openjdk-headless-11.0.6.10-1.el7_7 \
    unzip-6.0-20.el7 \
    which-2.20-7.el7 \
    # Use ss to view socket states. See #69947
    iproute-4.11.0-25.el7_7.2 \
    # The following aren't direct dependencies of WRES, are updates post-7.7.1908
    binutils-2.27-41.base.el7_7.1 \
    ca-certificates-2019.2.32-76.el7_7 \
    curl-7.29.0-54.el7_7.1 \
    device-mapper-event-1.02.158-2.el7_7.2 \
    device-mapper-libs-1.02.158-2.el7_7.2 \
    hostname-3.13-3.el7_7.1 \
    krb5-libs-1.15.1-37.el7_7.2 \
    libblkid-2.23.2-61.el7_7.1 \
    libcurl-7.29.0-54.el7_7.1 \
    libmount-2.23.2-61.el7_7.1 \
    libsmartcols-2.23.2-61.el7_7.1 \
    libuuid-2.23.2-61.el7_7.1 \
    nss-3.44.0-7.el7_7 \
    nss-softokn-3.44.0-8.el7_7 \
    nss-softokn-freebl-3.44.0-8.el7_7 \
    nss-sysinit-3.44.0-7.el7_7 \
    nss-tools-3.44.0-7.el7_7 \
    nss-util-3.44.0-4.el7_7 \
    procps-ng-3.3.10-26.el7_7.1 \
    systemd-219-67.el7_7.2 \
    systemd-libs-219-67.el7_7.2 \
    tzdata-2019c-1.el7 \
    util-linux-2.23.2-61.el7_7.1 \
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

