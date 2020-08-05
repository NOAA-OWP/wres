FROM centos:7.8.2003

RUN yum install -y \
    java-11-openjdk-headless-11.0.7.10-4.el7_8 \
    unzip-6.0-21.el7 \
    which-2.20-7.el7 \
    fontconfig-2.13.0-4.3.el7 \
    dejavu-fonts-common-2.33-6.el7 \
    dejavu-sans-mono-fonts-2.33-6.el7 \
    dejavu-sans-fonts-2.33-6.el7 \
    dejavu-serif-fonts-2.33-6.el7 \
    # Use ss to view socket states. See #69947
    iproute-4.11.0-25.el7_7.2 \
    # The following aren't direct dependencies of WRES, are updates post-7.8.2003
    bind-license-9.11.4-16.P2.el7_8.6 \
    binutils-2.27-43.base.el7_8.1 \
    dbus-1.10.24-14.el7_8 \
    dbus-libs-1.10.24-14.el7_8 \
    device-mapper-1.02.164-7.el7_8.2 \
    device-mapper-libs-1.02.164-7.el7_8.2 \
    systemd-219-73.el7_8.8 \
    systemd-libs-219-73.el7_8.8 \
    tzdata-2020a-1.el7 \
    yum-plugin-fastestmirror-1.1.31-54.el7_8 \
    yum-plugin-ovl-1.1.31-54.el7_8 \
    yum-utils-1.1.31-54.el7_8 \
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
