FROM redis:7.4.7-alpine3.21

# Uncomment the following lines if you want to build docker images with the container not being the docker Daemon
# Instead, very manually create user and group:
#RUN echo "wres_persister_docker:x:498:1370800073::/home/wres_persister_docker:" >> /etc/passwd \
#    && echo "user:!:1:0:99999:7:::" >> /etc/shadow \
#    && echo "wres:x:1370800073:" >> /etc/group \
#    && mkdir -p /home/wres_persister_docker \
#    && chown wres_persister_docker /home/wres_persister_docker

COPY redis.conf /etc/redis/redis.conf
COPY docker-healthcheck /usr/local/bin/

# The WRES-specific entry point script which executes redis-check-aof.
COPY wres-docker-entrypoint.sh /

HEALTHCHECK CMD ["docker-healthcheck"]

# Uncomment this line as well to set the user you added above
#USER wres_persister_docker

# Indicate that the WRES script is to be used. Note that that script
# will still be given the same arguments as the default script, that
# being the Redis configuration file location.
ENTRYPOINT [ "/wres-docker-entrypoint.sh" ]
