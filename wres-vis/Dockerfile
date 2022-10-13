# From a base image, make updates
FROM registry.access.redhat.com/ubi8/ubi:8.6-943.1665521450

RUN dnf install -y \
    java-11-openjdk-headless-1:11.0.16.1.1-1.el8_6 \
    unzip-6.0-46.el8 \
    fontconfig-2.13.1-4.el8 \
    dejavu-fonts-common-2.35-7.el8 \
    dejavu-sans-fonts-2.35-7.el8 \
    procps-ng-3.3.15-6.el8 \
    iproute-5.15.0-4.el8_6.1 \
    hostname-3.20-6.el8 \
    && dnf clean all

# Run the app as non-root, so create a non-root user.
RUN groupadd --gid 1370800073 wres \
    && useradd --uid 501 --gid 1370800073 wres_graphics_docker --home-dir /container_home

ARG version
WORKDIR /opt
COPY ./build/distributions/wres-vis-${version}.zip .
RUN unzip wres-vis-${version}.zip \
    && rm wres-vis-${version}.zip

WORKDIR /opt/wres-vis-${version}

VOLUME /container_home

# Should correspond to the port configured in WORKDIR/lib/conf/eventbroker.properties
EXPOSE 5673
USER wres_graphics_docker
CMD [ "bin/wres-vis" ]
