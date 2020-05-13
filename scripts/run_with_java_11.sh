#!/bin/sh

# Until system has java 11 available, we can get an azul version
# The risk here is that this version becomes vulnerable and does not receive
# updates except manually by updating this script. So this is intended to be a
# temporary measure until NOAA VLAB Jenkins and NWCAL test systems have java 11.

# The sha256sum is published alongside releases visible at
# https://www.azul.com/downloads/zulu-community/

zulu_java_sha256sum="df0de67998ac0c58b3c9e83c86e2a81daca05dc5adc189d942bc5d3f4691e749"
zulu_java_version="zulu11.39.15-ca-jdk11.0.7-linux_x64"
zulu_java_tarball="${zulu_java_version}.tar.gz"
zulu_java_url="https://cdn.azul.com/zulu/bin/${zulu_java_tarball}"

if [ ! -d ${zulu_java_version} ]
then
    wget ${zulu_java_url} -O ${zulu_java_tarball}

    # Verify the checksum, or exit if it fails
    echo "${zulu_java_sha256sum}  ${zulu_java_tarball}" | sha256sum -c - && echo "Verified checksum" || exit 1

    # Unpack Java
    tar xvf ${zulu_java_tarball}
else
    echo "Found existing zulu java at ${zulu_java_version}"
fi

old_zulu_java_version="zulu11.35.13-ca-jdk11.0.5-linux_x64"

if [ -d ${old_zulu_java_version} ]
then
    echo "Found old zulu java at ${old_zulu_java_version}, removing..."
    rm -rf ${old_zulu_java_version}
fi

# Export JAVA_HOME in order to use the unpacked Java
export JAVA_HOME=${zulu_java_version}

# Test java version is in fact 11
echo "The following should indicate 11 somewhere..."
$JAVA_HOME/bin/java -version

echo "Running command $1 with java 11, assuming it respects JAVA_HOME..."
$1
