FROM alpine:3.7
RUN apk --update add openjdk8-jre=8.151.12-r0

# Wish we could do the following, but busybox does not support long gids:
#RUN addgroup -g 1370800073 wres && adduser -D -u 499 -g 1370800073 wres_tasker_docker

# Instead, very manually create user and group:
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
EXPOSE 8585
USER wres_tasker_docker
CMD [ "bin/wres-tasker" ]

VOLUME /wres_secrets