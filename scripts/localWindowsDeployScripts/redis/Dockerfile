FROM redis:7.4-alpine3.20

# See comments in main/worker/root wres Dockerfile
# Instead, very manually create user and group:
RUN echo "wres_persister_docker:x:498:1370800073::/home/wres_persister_docker:" >> /etc/passwd \
    && echo "user:!:1:0:99999:7:::" >> /etc/shadow \
    && echo "wres:x:1370800073:" >> /etc/group \
    && mkdir -p /home/wres_persister_docker \
    && chown wres_persister_docker /home/wres_persister_docker

COPY redis.conf /etc/redis/redis.conf
COPY docker-healthcheck /usr/local/bin/

RUN apk add dos2unix
RUN dos2unix /usr/local/bin/docker-healthcheck

HEALTHCHECK CMD ["docker-healthcheck"]

USER wres_persister_docker

