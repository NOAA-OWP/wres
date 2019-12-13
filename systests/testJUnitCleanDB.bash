#!/bin/bash


if [ $# -lt 1 ]
then
	echo "Usage: $0 yyyymmdd-hhhhhhh"
	echo "Example: $0 20191212-d241d00"
	exit
fi
REVISION=$1
. ~/.bash_profile

wresZipDirectory=/wres_share/releases/archive
#REVISION=20191212-d241d00
echo "REVISION = $REVISION"

./gradlew cleanTest test  -PversionToTest=$REVISION -PwresZipDirectory=$wresZipDirectory -PtestJvmSystemProperties="-Dwres.useSSL=true -Dwres.username=$WRES_DB_USERNAMEJ -Dwres.url=$WRES_DB_HOSTNAMEJ -Dwres.databaseName=$WRES_DB_NAMEJ -Djava.awt.headless=true" --tests="Scenario000" --$WRES_LOG_LEVELJ
