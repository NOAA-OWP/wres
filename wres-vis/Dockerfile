# From a base image, make updates
FROM registry.access.redhat.com/ubi8/ubi:8.3-297

RUN dnf install -y \
    java-11-openjdk-headless-11.0.10.0.9-4.el8_3 \
    unzip-6.0-43.el8 \
    fontconfig-2.13.1-3.el8 \
    dejavu-fonts-common-2.35-6.el8 \
    dejavu-sans-fonts-2.35-6.el8 \
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
