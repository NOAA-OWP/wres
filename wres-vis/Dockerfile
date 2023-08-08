# From a base image, make updates
FROM registry.access.redhat.com/ubi8/ubi:8.8-1009

RUN dnf install -y \
    java-17-openjdk-headless-1:17.0.8.0.7-2.el8 \
    unzip-6.0-46.el8 \
    fontconfig-2.13.1-4.el8 \
    dejavu-fonts-common-2.35-7.el8 \
    dejavu-sans-fonts-2.35-7.el8 \
    procps-ng-3.3.15-13.el8 \
    iproute-5.18.0-1.1.el8_8 \
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

# Use an entrypoint script
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

VOLUME /container_home

# Should correspond to the port configured in WORKDIR/lib/conf/eventbroker.properties
EXPOSE 5673
USER wres_graphics_docker
ENTRYPOINT [ "/docker-entrypoint.sh" ]
