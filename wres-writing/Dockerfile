# From a base image, make updates
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
# Run the app as non-root, so create a non-root user.
#RUN groupadd --gid 1370800073 wres \
#    && useradd --uid 501 --gid 1370800073 wres_writing_docker --home-dir /container_home

ARG version
WORKDIR /opt
COPY ./build/distributions/wres-writing-${version}.zip .
RUN unzip wres-writing-${version}.zip \
    && rm wres-writing-${version}.zip \
    && find . -type d -exec chmod u+rwx,g+rx,g-w,o+rx,o-w {} \; \
    && find . -type f -exec chmod u+rw,g+r,g-w,o+r,o-w {} \;


WORKDIR /opt/wres-writing-${version}

# Use an entrypoint script
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh


VOLUME /container_home

# Should correspond to the port configured in WORKDIR/lib/conf/eventbroker.properties
EXPOSE 5673

# Uncomment this line as well to set the user you added above
#USER wres_writing_docker
ENTRYPOINT [ "/docker-entrypoint.sh" ]
