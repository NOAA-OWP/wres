FROM centos:7.5.1804

RUN yum install -y java-1.8.0-openjdk-headless-1.8.0.191.b12-0.el7_5 unzip-6.0-19.el7 which-2.20-7.el7

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
USER wres_docker

CMD [ "bin/wres-worker", "/usr/bin/wres" ]

VOLUME /mnt/wres_share
VOLUME /wres_secrets
