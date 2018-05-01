FROM rabbitmq:3.7.4-management-alpine

# Wish we could do the following, but busybox does not support long gids:
#RUN addgroup -g 1370800073 wres && adduser -D -u 498 -g 1370800073 wres_docker

# Instead, very manually create user and group:
RUN echo "wres_docker:x:498:1370800073::/home/wres_docker:" >> /etc/passwd \
    && echo "user:!:1:0:99999:7:::" >> /etc/shadow \
    && echo "wres:x:1370800073:" >> etc/group \
    && mkdir /home/wres_docker \
    && chown wres_docker: /home/wres_docker

WORKDIR /opt/wres-broker
COPY nonsrc/*x509*pem ./
RUN chown wres_docker:wres . && chown wres_docker:wres *

WORKDIR /etc/rabbitmq
COPY nonsrc/rabbitmq.conf .
COPY nonsrc/definitions.json .

#WORKDIR /opt/wres-broker/bin

# Create a script that helps read the key from an env var set at runtime
#COPY nonsrc/wres-broker-setup.sh .
#RUN chown wres_docker:wres . && chown wres_docker:wres *

USER wres_docker

# NOTE: Must specify a volume at runtime having this key
#VOLUME /opt/wres-broker/***REMOVED***wres-broker_server_secret_rsa_key.pem

#CMD [ "/bin/sh", "/opt/wres-broker/bin/wres-broker-setup.sh" ]
