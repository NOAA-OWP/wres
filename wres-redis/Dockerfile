FROM redis:6.2.4-alpine3.13

RUN apk add --no-cache musl=1.2.2-r1 \
    musl-utils=1.2.2-r1

COPY docker-healthcheck /usr/local/bin/

HEALTHCHECK CMD ["docker-healthcheck"]
