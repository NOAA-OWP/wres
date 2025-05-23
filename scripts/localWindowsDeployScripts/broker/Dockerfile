FROM rabbitmq:3.13.6-management-alpine

# Wish we could do the following, but busybox does not support long gids:
#RUN addgroup -g 1370800073 wres && adduser -D -u 498 -g 1370800073 wres_docker

# Instead, very manually create user and group:
RUN echo "wres_docker:x:498:1370800073::/home/wres_docker:" >> /etc/passwd \
    && echo "user:!:1:0:99999:7:::" >> /etc/shadow \
    && echo "wres:x:1370800073:" >> /etc/group \
    && mkdir /home/wres_docker \
    && chown wres_docker /home/wres_docker

# Enable authentication with x509 client certificates (requires some conf too):
RUN rabbitmq-plugins enable --offline rabbitmq_auth_mechanism_ssl

RUN apk add dos2unix
COPY docker-healthcheck /usr/local/bin/
RUN dos2unix /usr/local/bin/docker-healthcheck

WORKDIR /etc/rabbitmq
COPY nonsrc/rabbitmq.conf .
COPY nonsrc/definitions.json .


USER wres_docker

VOLUME /wres_secrets

# Concert windows line ending to UNIX


HEALTHCHECK CMD ["docker-healthcheck"]
