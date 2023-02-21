FROM redis:6.2.10-alpine3.17

COPY redis.conf /etc/redis/redis.conf
COPY docker-healthcheck /usr/local/bin/

HEALTHCHECK CMD ["docker-healthcheck"]
