FROM centos:8.3.2011

RUN dnf install -y \
    java-11-openjdk-headless-11.0.9.11-3.el8_3 \
    unzip-6.0-43.el8 \
    which-2.21-12.el8 \
    fontconfig-2.13.1-3.el8 \
    dejavu-fonts-common-2.35-6.el8 \
    dejavu-sans-mono-fonts-2.35-6.el8 \
    dejavu-sans-fonts-2.35-6.el8 \
    dejavu-serif-fonts-2.35-6.el8 \
    # The following aren't direct dependencies of WRES, are updates post-8.3.2011
    bind-export-libs-32:9.11.20-5.el8_3.1 \
    curl-7.61.1-14.el8_3.1 \
    dracut-049-95.git20200804.el8_3.4 \
    dracut-network-049-95.git20200804.el8_3.4 \
    dracut-squash-049-95.git20200804.el8_3.4 \
    gnutls-3.6.14-7.el8_3 \
    iptables-libs-1.8.4-15.el8_3.3 \
    kexec-tools-2.0.20-34.el8_3.1 \
    libcurl-minimal-7.61.1-14.el8_3.1 \
    openssl-1.1.1g-15.el8_3 \
    openssl-libs-1.1.1g-15.el8_3 \
    systemd-239-41.el8_3.1 \
    systemd-libs-239-41.el8_3.1 \
    systemd-pam-239-41.el8_3.1 \
    systemd-udev-239-41.el8_3.1 \
    tzdata-2021a-1.el8 \
    tzdata-java-2021a-1.el8 \
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
