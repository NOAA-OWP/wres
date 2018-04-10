FROM alpine:3.7
RUN apk --update add openjdk8-jre=8.151.12-r0 ttf-dejavu=2.37-r0

# Wish we could do the following, but busybox does not support long gids:
#RUN addgroup -g 1370800073 wres && adduser -D -u 498 -g 1370800073 wres_docker

# Instead, very manually create user and group:
RUN echo "wres_docker:x:498:1370800073::/home/wres_docker:" >> /etc/passwd \
    && echo "user:!:1:0:99999:7:::" >> /etc/shadow \
    && echo "wres:x:1370800073:" >> etc/group \
    && mkdir /home/wres_docker \
    && chown wres_docker: /home/wres_docker

ARG version
COPY ./build/distributions/wres-${version}.zip /opt/
RUN cd /opt \
    && unzip wres-${version}.zip \
    && rm wres-${version}.zip \
    && ln -s /opt/wres-${version}/bin/wres /usr/bin/wres

# WRES above is a one-run-one-database, one-run-one-evaluation, use light shim:
ARG worker_version
COPY ./wres-worker/build/distributions/wres-worker-${worker_version}.zip /opt/
RUN cd /opt \
    && unzip wres-worker-${worker_version}.zip \
    && rm wres-worker-${worker_version}.zip \
    && ln -s /opt/wres-worker-${worker_version}/bin/wres-worker /usr/bin/wres-worker

USER wres_docker
CMD [ "/usr/bin/wres-worker", "/usr/bin/wres" ]
