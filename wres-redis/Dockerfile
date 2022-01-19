FROM redis:6.2.6-alpine3.15

COPY redis.conf /etc/redis/redis.conf
COPY docker-healthcheck /usr/local/bin/

HEALTHCHECK CMD ["docker-healthcheck"]
