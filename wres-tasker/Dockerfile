FROM alpine:3.7
RUN apk --update add openjdk8-jre=8.151.12-r0

RUN adduser -D -u 499 wres_tasker_docker

ARG version
COPY ./build/distributions/wres-tasker-${version}.zip /opt/
RUN cd /opt \
    && unzip wres-tasker-${version}.zip \
    && rm wres-tasker-${version}.zip \
    && ln -s /opt/wres-tasker-${version}/bin/wres-tasker /usr/bin/wres-tasker

EXPOSE 8585
USER wres_tasker_docker
CMD [ "/usr/bin/wres-tasker" ]
