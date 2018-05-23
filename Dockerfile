FROM openjdk:8u171-jre-slim-stretch

# The magic group id matches what we have as NWCAL's "wres" gid.
# The justification for worrying about gid is file io needs, e.g. secret keys.
RUN addgroup --gid 1370800073 wres \
    && adduser --system --uid 498 --gid 1370800073 wres_docker

# Disable assistive technologies in headless JRE
RUN sed -i 's/^assistive_technologies=/#assistive_technologies=/' /etc/java-8-openjdk/accessibility.properties

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
