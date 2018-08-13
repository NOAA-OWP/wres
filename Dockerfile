FROM azul/zulu-openjdk-alpine:8u181-8.31.0.1

# GraphGen requires fonts
RUN apk --update add ttf-dejavu=2.37-r0

# The magic group id matches what we have as NWCAL's "wres" gid.
# The justification for worrying about gid is file io needs, e.g. secret keys.
# Wish we could do the following, but busybox does not support long gids:
#RUN addgroup --gid 1370800073 wres \
#    && adduser --system --uid 498 --gid 1370800073 wres_docker

# Instead, very manually create user and group:
RUN echo "wres_docker:x:498:1370800073::/home/wres_docker:" >> /etc/passwd \
    && echo "user:!:1:0:99999:7:::" >> /etc/shadow \
    && echo "wres:x:1370800073:" >> etc/group \
    && mkdir /home/wres_docker \
    && chown wres_docker: /home/wres_docker

# Specifies which version of the main WRES version to use.
ARG version
WORKDIR /opt
COPY ./build/distributions/wres-${version}.zip .
RUN unzip wres-${version}.zip \
    && rm wres-${version}.zip \
    && ln -s /opt/wres-${version}/bin/wres /usr/bin/wres

# WRES above is a one-run-one-database, one-run-one-evaluation, use light shim:
ARG worker_version
WORKDIR /opt
COPY ./wres-worker/build/distributions/wres-worker-${worker_version}.zip .
RUN unzip wres-worker-${worker_version}.zip \
    && rm wres-worker-${worker_version}.zip

WORKDIR /opt/wres-worker-${worker_version}
USER wres_docker

CMD [ "bin/wres-worker", "/usr/bin/wres" ]

VOLUME /mnt/wres_share
VOLUME /wres_secrets
