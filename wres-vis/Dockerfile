# From a base image, make updates
FROM centos:8.3.2011

RUN dnf install -y \
    java-11-openjdk-headless-11.0.9.11-2.el8_3 \
    unzip-6.0-43.el8 \
    which-2.21-12.el8 \
    fontconfig-2.13.1-3.el8 \
    dejavu-fonts-common-2.35-6.el8 \
    dejavu-sans-mono-fonts-2.35-6.el8 \
    dejavu-sans-fonts-2.35-6.el8 \
    dejavu-serif-fonts-2.35-6.el8 \
    # The following aren't direct dependencies of WRES, are updates post-8.3.2011
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

# Should correspond to the port configured in WORKDIR/lib/conf/eventbroker.properties
EXPOSE 5673
USER wres_graphics_docker
CMD [ "bin/wres-vis" ]
