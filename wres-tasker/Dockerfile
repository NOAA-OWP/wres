FROM registry.access.redhat.com/ubi8/ubi:8.10-1304

RUN dnf install -y \
    java-17-openjdk-headless-1:17.0.15.0.6-2.el8 \
    unzip-6.0-48.el8_10 \
    procps-ng-3.3.15-14.el8 \
    iproute-6.2.0-6.el8_10 \
    hostname-3.20-6.el8 \
    && dnf clean all

# Uncomment the following lines if you want to build docker images with the container not being the docker Daemon
# See comments in main/worker/root wres Dockerfile
#RUN groupadd --gid 1370800073 wres \
#    && useradd --uid 499 --gid 1370800073 wres_tasker_docker

ARG version
WORKDIR /opt
COPY ./build/distributions/wres-tasker-${version}.zip .
RUN unzip wres-tasker-${version}.zip \
    && rm wres-tasker-${version}.zip \
    && find . -type d -exec chmod u+rwx,g+rx,g-w,o+rx,o-w {} \; \
    && find . -type f -exec chmod u+rw,g+r,g-w,o+r,o-w {} \;
 

WORKDIR /opt/wres-tasker-${version}

# Use an entrypoint script
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8443
# Uncomment this line as well to set the user you added above
#USER wres_tasker_docker

# Remember, to exec into a container and open a shell, use: 
# docker run -ti --entrypoint /bin/bash container_id
# Without the --entrypoint, the entrypoint script below will run
ENTRYPOINT [ "/docker-entrypoint.sh" ]

VOLUME /wres_secrets

