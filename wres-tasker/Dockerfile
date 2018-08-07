FROM azul/zulu-openjdk-alpine:8u181-8.31.0.1

# See comments in main wres Dockerfile
RUN echo "wres_tasker_docker:x:499:1370800073::/home/wres_tasker_docker:" >> /etc/passwd \
    && echo "user:!:1:0:99999:7:::" >> /etc/shadow \
    && echo "wres:x:1370800073:" >> etc/group \
    && mkdir /home/wres_tasker_docker \
    && chown wres_tasker_docker: /home/wres_tasker_docker

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