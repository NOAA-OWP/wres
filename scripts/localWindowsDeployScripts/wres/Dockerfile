FROM registry.access.redhat.com/ubi8/ubi:8.8-1009

RUN dnf install -y \
    java-17-openjdk-headless-1:17.0.8.0.7-2.el8 \
    unzip-6.0-46.el8 \
    fontconfig-2.13.1-4.el8 \
    dejavu-fonts-common-2.35-7.el8 \
    dejavu-sans-fonts-2.35-7.el8 \
    procps-ng-3.3.15-14.el8 \
    iproute-6.2.0-5.el8_9 \
    hostname-3.20-6.el8 \
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
    && ln -s /opt/wres-${version}/bin/wres /usr/bin/wres \
    && cp /opt/wres-${version}/lib/conf/wres_jfr.jfc /usr/lib/jvm/jre/lib/jfr

# WRES above is a one-run-one-database, one-run-one-evaluation, use light shim:
ARG worker_version
WORKDIR /opt
COPY ./wres-worker/build/distributions/wres-worker-${worker_version}.zip .
RUN unzip wres-worker-${worker_version}.zip \
    && rm wres-worker-${worker_version}.zip

WORKDIR /opt/wres-worker-${worker_version}

# In order to set umask for file sharing, use a wrapper script, see #56790
COPY ./scripts/docker-entrypoint.sh .

# Concert windows line ending to UNIX
RUN dnf install -y dos2unix
RUN dos2unix docker-entrypoint.sh

USER wres_docker

CMD [ "./docker-entrypoint.sh" ]

VOLUME /mnt/wres_share
VOLUME /wres_secrets

HEALTHCHECK --interval=30s --timeout=15s CMD curl -f --retry 1 --max-time 5 http://localhost:8010/evaluation/heartbeat || bash -c 'kill 1'