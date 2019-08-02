#!/bin/bash

# This bash program is intended for use by admins of a WRES cluster.
# This script runs a clean database on the single db used in an environment.
# Intended to be run from a directory one level deeper than wres/scripts.

env_suffix=-dev
user_name=anonymous

echo "We are using the $env_suffix environment in this program."

read -n1 -r -p "Please ctrl-c if that is not correct, any key otherwise..." key

wres_ca_file=../cacerts/wres_ca_x509_cert.pem

if [ -f $wres_ca_file ]
then
    echo "Found the WRES CA file at $wres_ca_file"
else
    echo "Did not find the WRES CA file at $wres_ca_file"
    exit 1
fi

post_result=$( curl -i -s --cacert $wres_ca_file --data "userName=${user_name}&projectConfig=none&verb=cleandatabase" https://***REMOVED***wres${env_suffix}.***REMOVED***.***REMOVED***/job | tr -d '\r' )
post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )
echo "The last status code in the response was $post_result_http_code"

if [ "$post_result_http_code" -eq "201" ] || [ "$post_result_http_code" -eq "200" ]
then
    echo "The response code was successful: $post_result_http_code"
else
    echo "The response code was NOT 201 nor 200, failed to create, exiting..."
    exit 2
fi

job_location=$( echo -n "$post_result" | grep Location | cut -d' ' -f2 )
echo "The location of the resource created by server was $job_location"

jobs_failed=0

evaluation_status=""

while [ "$evaluation_status" != "COMPLETED_REPORTED_SUCCESS" ] \
    && [ "$evaluation_status" != "COMPLETED_REPORTED_FAILURE" ]
do
    sleep 0.5
    evaluation_status=$( curl --cacert $wres_ca_file $job_location/status -s | tr -d '\r' )
done

if [ "$evaluation_status" == "COMPLETED_REPORTED_SUCCESS" ]
then
    echo "Clean succeeded for $job_location"
else
    echo "CLEAN FAILED FOR $job_location"
fi

if [ "$jobs_failed" -ne "0" ]
then
    echo "There were $jobs_failed FAILED MIGRATIONS! See above..."
    exit $jobs_failed
fi

exit 0
