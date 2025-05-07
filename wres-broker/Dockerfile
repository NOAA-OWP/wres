FROM rabbitmq:3.13.7-management-alpine

# Uncomment the following lines if you want to build docker images with the container not being the docker Daemon
# Instead, very manually create user and group:
#RUN echo "wres_docker:x:498:1370800073::/home/wres_docker:" >> /etc/passwd \
#    && echo "user:!:1:0:99999:7:::" >> /etc/shadow \
#    && echo "wres:x:1370800073:" >> /etc/group \
#    && mkdir /home/wres_docker \
#    && chown wres_docker /home/wres_docker


# Enable authentication with x509 client certificates (requires some conf too):
RUN rabbitmq-plugins enable --offline rabbitmq_auth_mechanism_ssl
RUN rabbitmq-plugins enable --offline rabbitmq_management

WORKDIR /etc/rabbitmq
COPY nonsrc/rabbitmq.conf .
COPY nonsrc/definitions.json .
COPY docker-healthcheck /usr/local/bin/

# Uncomment this line as well to set the user you added above
#USER wres_docker

VOLUME /wres_secrets

HEALTHCHECK CMD ["docker-healthcheck"]
