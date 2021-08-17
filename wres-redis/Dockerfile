FROM redis:6.2.5-alpine3.14

COPY redis.conf /etc/redis/redis.conf
COPY docker-healthcheck /usr/local/bin/

HEALTHCHECK CMD ["docker-healthcheck"]
