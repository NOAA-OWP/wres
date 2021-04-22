FROM redis:6.2.2-alpine3.13

COPY docker-healthcheck /usr/local/bin/

HEALTHCHECK CMD ["docker-healthcheck"]
