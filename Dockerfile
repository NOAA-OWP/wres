FROM centos:7.7.1908

RUN yum install -y \
    java-11-openjdk-headless-11.0.5.10-0.el7_7 \
    unzip-6.0-20.el7 \
    which-2.20-7.el7 \
    fontconfig-2.13.0-4.3.el7 \
    dejavu-fonts-common-2.33-6.el7 \
    dejavu-sans-mono-fonts-2.33-6.el7 \
    dejavu-sans-fonts-2.33-6.el7 \
    dejavu-serif-fonts-2.33-6.el7 \
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
    nss-softokn--3.44.0-8.el7_7 \
    nss-softokn-freebl-3.44.0-8.el7_7 7 \
    nss-sysinit-3.44.0-7.el7_7 \
    nss-tools-3.44.0-7.el7_7 \
    nss-util-3.44.0-4.el7_7 \
    procps-ng-3.3.10-26.el7_7.1 \
    systemd-219-67.el7_7.2 \
    systemd-libs-219-67.el7_7.2 \
    tzdata-2019c-1.el7 \
    util-linux-2.23.2-61.el7_7.1 \
    && yum clean all

# For examples of the following for alpine or debian, see git history.
RUN groupadd --gid 1370800073 wres \
    && useradd --uid 498 --gid 1370800073 wres_docker --home-dir /container_home

# Specifies which version of the main WRES version to use.
ARG version
WORKDIR /opt
COPY ./build/distributions/wres-${version}.zip .
RUN unzip wres-${version}.zip \
    && rm wres-${version}.zip \
    && ln -s /opt/wres-${version}/bin/wres /usr/bin/wres

# WRES above is a one-run-one-database, one-run-one-evaluation, use light shim:
ARG worker_version
WORKDIR /opt
COPY ./wres-worker/build/distributions/wres-worker-${worker_version}.zip .
RUN unzip wres-worker-${worker_version}.zip \
    && rm wres-worker-${worker_version}.zip

WORKDIR /opt/wres-worker-${worker_version}

# In order to set umask for file sharing, use a wrapper script, see #56790
COPY ./scripts/docker-entrypoint.sh .

USER wres_docker

CMD [ "./docker-entrypoint.sh" ]

VOLUME /mnt/wres_share
VOLUME /wres_secrets
