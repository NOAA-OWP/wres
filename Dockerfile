FROM alpine:3.7
RUN apk --update add openjdk8-jre=8.151.12-r0
CMD ["/usr/bin/java", "-version"]
