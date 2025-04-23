FROM registry.access.redhat.com/ubi8/ubi:8.10-1184.1741863532

RUN dnf install -y \
    java-17-openjdk-headless \
    unzip \
    fontconfig \
    dejavu-fonts-common \
    dejavu-sans-fonts \
    procps-ng \
    iproute \
    hostname \
    && dnf clean all

# For examples of the following for alpine or debian, see git history.
RUN groupadd --gid 1370800073 wres \
    && useradd --uid 498 --gid 1370800073 wres_docker --home-dir /container_home

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

USER wres_docker

CMD [ "./docker-entrypoint.sh" ]

VOLUME /mnt/wres_share
VOLUME /wres_secrets
