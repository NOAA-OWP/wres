FROM centos:8.2.2004

RUN dnf install -y \
    java-11-openjdk-headless-11.0.9.11-0.el8_2 \
    unzip-6.0-43.el8 \
    which-2.21-12.el8 \
    fontconfig-2.13.1-3.el8 \
    dejavu-fonts-common-2.35-6.el8 \
    dejavu-sans-mono-fonts-2.35-6.el8 \
    dejavu-sans-fonts-2.35-6.el8 \
    dejavu-serif-fonts-2.35-6.el8 \
    # The following aren't direct dependencies of WRES, are updates post-8.2.2004
    bind-export-libs-9.11.13-6.el8_2.1 \
    ca-certificates-2020.2.41-80.0.el8_2 \
    centos-gpg-keys-8.2-2.2004.0.2.el8 \
    centos-release-8.2-2.2004.0.2.el8 \
    centos-repos-8.2-2.2004.0.2.el8 \
    dbus-1.12.8-10.el8_2 \
    dbus-common-1.12.8-10.el8_2 \
    dbus-daemon-1.12.8-10.el8_2 \
    dbus-libs-1.12.8-10.el8_2 \
    dbus-tools-1.12.8-10.el8_2 \
    dnf-4.2.17-7.el8_2 \
    dnf-data-4.2.17-7.el8_2 \
    gnutls-3.6.8-11.el8_2 \
    iptables-libs-1.8.4-10.el8_2.1 \
    libdnf-0.39.1-6.el8_2 \
    libnghttp2-1.33.0-3.el8_2.1 \
    librepo-1.11.0-3.el8_2 \
    python3-dnf-4.2.17-7.el8_2 \
    python3-hawkey-0.39.1-6.el8_2 \
    python3-libdnf-0.39.1-6.el8_2 \
    systemd-239-31.el8_2.2 \
    systemd-libs-239-31.el8_2.2 \
    systemd-pam-239-31.el8_2.2 \
    systemd-udev-239-31.el8_2.2 \
    tzdata-2020d-1.el8 \
    tzdata-java-2020d-1.el8 \
    yum-4.2.17-7.el8_2 \
    zlib-1.2.11-16.el8_2 \
    && dnf clean all

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
