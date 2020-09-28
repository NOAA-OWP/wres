FROM centos:8.2.2004

RUN dnf install -y \
    java-11-openjdk-headless-11.0.8.10-0.el8_2 \
    unzip-6.0-43.el8 \
    which-2.21-12.el8 \
    # The following aren't direct dependencies of WRES, are updates post-8.2.2004
    bind-export-libs-9.11.13-6.el8_2.1 \
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
    yum-4.2.17-7.el8_2 \
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

