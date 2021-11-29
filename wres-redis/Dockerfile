FROM redis:6.2.6-alpine3.14

RUN apk add --no-cache busybox=1.33.1-r6

COPY redis.conf /etc/redis/redis.conf
COPY docker-healthcheck /usr/local/bin/

HEALTHCHECK CMD ["docker-healthcheck"]
