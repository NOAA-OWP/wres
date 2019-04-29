FROM centos:7.6.1810

RUN yum install -y \
    java-11-openjdk-headless-11.0.3.7-0.el7_6 \
    unzip-6.0-19.el7 \
    which-2.20-7.el7 \
    # The following aren't direct dependencies of WRES, are updates post-1810:
    bind-license-9.9.4-73.el7_6 \
    dbus-1.10.24-13.el7_6 \
    dbus-libs-1.10.24-13.el7_6 \
    device-mapper-1.02.149-10.el7_6.3 \
    device-mapper-event-libs-1.02.149-10.el7_6.3 \
    glibc-2.17-260.el7_6.4 \
    glibc-common-2.17-260.el7_6.4 \
    krb5-libs-1.15.1-37.el7_6 \
    libblkid-2.23.2-59.el7_6.1 \
    libgcc-4.8.5-36.el7_6.1 \
    libmount-2.23.2-59.el7_6.1 \
    libsmartcols-2.23.2-59.el7_6.1 \
    libssh2-1.4.3-12.el7_6.2 \
    libstdc++-4.8.5-36.el7_6.1 \
    libuuid-2.23.2-59.el7_6.1 \
    nss-3.36.0-7.1.el7_6 \
    nss-pem-1.0.3-5.el7_6.1 \
    nss-sysinit-3.36.0-7.1.el7_6 \
    nss-tools-3.36.0-7.1.el7_6 \
    nss-util-3.36.0-1.1.el7_6 \
    openldap-2.4.44-21.el7_6 \
    openssl-libs-1.0.2k-16.el7_6.1 \
    python-2.7.5-77.el7_6 \
    python-libs-2.7.5-77.el7_6 \
    shadow-utils-4.1.5.1-25.el7_6.1 \
    systemd-219-62.el7_6.5 \
    systemd-libs-219-62.el7_6.5 \
    tzdata-2019a-1.el7 \
    tzdata-java-2019a-1.el7 \
    util-linux-2.23.2-59.el7_6.1

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
