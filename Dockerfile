FROM alpine:3.7
RUN apk --update add openjdk8-jre=8.151.12-r0
ENV wres_version 20180330-ac044fb
COPY ./build/distributions/wres-${wres_version}.zip /opt/
RUN cd /opt && unzip wres-${wres_version}.zip && rm wres-${wres_version}.zip && ln -s /opt/wres-${wres_version}/bin/wres /usr/bin/wres
CMD ["/usr/bin/wres"]
