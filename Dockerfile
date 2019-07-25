FROM centos:7.6.1810

RUN yum install -y \
    java-11-openjdk-headless-11.0.4.11-0.el7_6 \
    unzip-6.0-19.el7 \
    which-2.20-7.el7 \
    fontconfig-2.13.0-4.3.el7 \
    dejavu-fonts-common-2.33-6.el7 \
    dejavu-sans-mono-fonts-2.33-6.el7 \
    dejavu-sans-fonts-2.33-6.el7 \
    dejavu-serif-fonts-2.33-6.el7 \
    # The following aren't direct dependencies of WRES, are updates post-1810:
    bind-license-9.9.4-74.el7_6.1 \
    dbus-1.10.24-13.el7_6 \
    dbus-libs-1.10.24-13.el7_6 \
    device-mapper-1.02.149-10.el7_6.8 \
    device-mapper-event-libs-1.02.149-10.el7_6.8 \
    device-mapper-libs-1.02.149-10.el7_6.8 \
    glib2-2.56.1-4.el7_6 \
    glibc-2.17-260.el7_6.6 \
    glibc-common-2.17-260.el7_6.6 \
    krb5-libs-1.15.1-37.el7_6 \
    libblkid-2.23.2-59.el7_6.1 \
    libgcc-4.8.5-36.el7_6.2 \
    libmount-2.23.2-59.el7_6.1 \
    libsmartcols-2.23.2-59.el7_6.1 \
    libssh2-1.4.3-12.el7_6.2 \
    libstdc++-4.8.5-36.el7_6.2 \
    libuuid-2.23.2-59.el7_6.1 \
    nss-3.36.0-7.1.el7_6 \
    nss-pem-1.0.3-5.el7_6.1 \
    nss-sysinit-3.36.0-7.1.el7_6 \
    nss-tools-3.36.0-7.1.el7_6 \
    nss-util-3.36.0-1.1.el7_6 \
    openldap-2.4.44-21.el7_6 \
    openssl-libs-1.0.2k-16.el7_6.1 \
    python-2.7.5-80.el7_6 \
    python-libs-2.7.5-80.el7_6 \
    shadow-utils-4.1.5.1-25.el7_6.1 \
    systemd-219-62.el7_6.7 \
    systemd-libs-219-62.el7_6.7 \
    tzdata-2019b-1.el7 \
    tzdata-java-2019b-1.el7 \
    util-linux-2.23.2-59.el7_6.1 \
    vim-minimal-7.4.160-6.el7_6

# For examples of the following for alpine or debian, see git history.
RUN groupadd --gid 1370800073 wres \
    && useradd --uid 498 --gid 1370800073 wres_docker

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
