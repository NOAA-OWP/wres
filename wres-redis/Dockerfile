FROM redis:6.2.13-alpine3.18

COPY redis.conf /etc/redis/redis.conf
COPY docker-healthcheck /usr/local/bin/

HEALTHCHECK CMD ["docker-healthcheck"]
