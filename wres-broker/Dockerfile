FROM rabbitmq:3.9.8-management-alpine

# Wish we could do the following, but busybox does not support long gids:
#RUN addgroup -g 1370800073 wres && adduser -D -u 498 -g 1370800073 wres_docker

# Instead, very manually create user and group:
RUN echo "wres_docker:x:498:1370800073::/home/wres_docker:" >> /etc/passwd \
    && echo "user:!:1:0:99999:7:::" >> /etc/shadow \
    && echo "wres:x:1370800073:" >> /etc/group \
    && mkdir /home/wres_docker \
    && chown wres_docker: /home/wres_docker

# Enable authentication with x509 client certificates (requires some conf too):
RUN rabbitmq-plugins enable --offline rabbitmq_auth_mechanism_ssl

WORKDIR /etc/rabbitmq
COPY nonsrc/rabbitmq.conf .
COPY nonsrc/definitions.json .
COPY docker-healthcheck /usr/local/bin/

USER wres_docker

VOLUME /wres_secrets

HEALTHCHECK CMD ["docker-healthcheck"]
