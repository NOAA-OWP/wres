FROM registry.access.redhat.com/ubi8/ubi:8.10-1184.1741863532

RUN dnf install -y \
    java-17-openjdk-headless-1:17.0.15.0.6-2.el8.x86_64 \
    unzip-6.0-47.el8_10.x86_64 \
    fontconfig-2.13.1-4.el8 \
    dejavu-fonts-common-2.35-7.el8 \
    dejavu-sans-fonts-2.35-7.el8 \
    procps-ng-3.3.15-14.el8 \
    iproute-6.2.0-6.el8_10.x86_64 \
    hostname-3.20-6.el8 \
    && dnf clean all

# Uncomment the following lines if you want to build docker images with the container not being the docker Daemon
#RUN groupadd --gid 1370800073 wres \
#    && useradd --uid 498 --gid 1370800073 wres_docker --home-dir /container_home

# Specifies which version of the main WRES version to use.
ARG version
WORKDIR /opt
COPY ./wres-worker/dist/lib/conf/inner_logback.xml .
COPY ./build/distributions/wres-${version}.zip .
RUN unzip wres-${version}.zip \
    && rm wres-${version}.zip \
    && ln -s /opt/wres-${version}/bin/wres /usr/bin/wres \
    && cp /opt/wres-${version}/lib/conf/wres_jfr.jfc /usr/lib/jvm/jre/lib/jfr \
    && find . -type d -exec chmod u+rwx,g+rx,g-w,o+rx,o-w {} \; \
    && find . -type f -exec chmod u+rw,g+r,g-w,o+r,o-w {} \;

# WRES above is a one-run-one-database, one-run-one-evaluation, use light shim:
ARG worker_version
WORKDIR /opt
COPY ./wres-worker/build/distributions/wres-worker-${worker_version}.zip .
RUN unzip wres-worker-${worker_version}.zip \
    && rm wres-worker-${worker_version}.zip  \
    && find . -type d -exec chmod u+rwx,g+rx,g-w,o+rx,o-w {} \; \
    && find . -type f -exec chmod u+rw,g+r,g-w,o+r,o-w {} \;

WORKDIR /opt/wres-worker-${worker_version}

# In order to set umask for file sharing, use a wrapper script, see #56790
COPY ./scripts/docker-entrypoint.sh .

# Uncomment this line as well to set the user you added above
# USER wres_docker

CMD [ "./docker-entrypoint.sh" ]

VOLUME /mnt/wres_share
VOLUME /wres_secrets
