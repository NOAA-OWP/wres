FROM alpine:3.7
RUN apk --update add openjdk8-jre=8.151.12-r0

RUN adduser -D -u 499 wres_tasker_docker

ARG version
WORKDIR /opt
COPY ./build/distributions/wres-tasker-${version}.zip .
RUN unzip wres-tasker-${version}.zip \
    && rm wres-tasker-${version}.zip

WORKDIR /opt/wres-tasker-${version}
EXPOSE 8585
USER wres_tasker_docker
ENV JAVA_OPTS -Dwres.broker=***REMOVED***wres-broker
CMD [ "bin/wres-tasker" ]
