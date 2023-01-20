#!/bin/sh

# Until the build system has java 17 available or gradle toolchains can
# be used successfully on the build server, we can get an azul version.
# The risk here is that this version becomes vulnerable and does not receive
# updates except manually by updating this script. So this is intended to be a
# temporary measure. In reality, it has persisted for several years now.

# The sha256sum is published alongside releases visible at
# https://www.azul.com/downloads/zulu-community/
zulu_java_sha256sum="2867572c5af67d7bf4c53bf9d96c35977eebdfdbf26202c2dc7a1acbbea3f6b7"
zulu_java_version="zulu17.40.19-ca-jdk17.0.6-linux_x64"
zulu_java_tarball="${zulu_java_version}.tar.gz"
zulu_java_url="https://cdn.azul.com/zulu/bin/${zulu_java_tarball}"

# When updating, declare the old one here for removal
old_zulu_java_version="zulu11.54.25-ca-jdk11.0.14.1-linux_x64"
old_zulu_java_version_tarball="${old_zulu_java_version}.tar.gz"

if [ ! -d ${zulu_java_version} ]
then
    wget ${zulu_java_url} -O ${zulu_java_tarball}

    # Verify the checksum, or exit if it fails
    echo "${zulu_java_sha256sum}  ${zulu_java_tarball}" | sha256sum -c - && echo "Verified checksum" || exit 1

    # Unpack Java
    tar xvf ${zulu_java_tarball}

    # Remove tarball
    rm -rf ${zulu_java_tarball}
else
    echo "Found existing zulu java at ${zulu_java_version}"
fi

if [ -d ${old_zulu_java_version} ]
then
    echo "Found old zulu java at ${old_zulu_java_version}, removing..."
    rm -rf ${old_zulu_java_version}
    rm -rf ${old_zulu_java_version_tarball}
fi

# Export JAVA_HOME in order to use the unpacked Java
export JAVA_HOME=${zulu_java_version}

# Test java version is in fact 17
echo "The following should indicate 17 somewhere..."
$JAVA_HOME/bin/java -version

echo "Running command $1 with java 17, assuming it respects JAVA_HOME..."
$1
