FROM centos:7.5.1804

RUN yum install -y java-1.8.0-openjdk-headless-1.8.0.191.b12-0.el7_5 unzip-6.0-19.el7

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
