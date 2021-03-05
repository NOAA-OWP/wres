FROM centos:8.3.2011

RUN dnf install -y \
    java-11-openjdk-headless-11.0.9.11-3.el8_3 \
    unzip-6.0-43.el8 \
    which-2.21-12.el8 \
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
    openssl-libs-1.1.1g-12.el8_3 \
    systemd-239-41.el8_3.1 \
    systemd-libs-239-41.el8_3.1 \
    systemd-pam-239-41.el8_3.1 \
    systemd-udev-239-41.el8_3.1 \
    tzdata-2021a-1.el8 \
    tzdata-java-2021a-1.el8 \
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

