#!/bin/bash

# This bash program is intended for use by admins of a WRES cluster.
# After deploying a new version of WRES, this script runs migration.
# Intended to be run from a directory one level deeper than wres/scripts.

env_suffix=-dev

echo "We are using the $env_suffix environment in this program."

users=$( grep "\.${env_suffix}\." ../../wres-tasker/nonsrc/users.properties \
         | cut -d'.' -f4 | cut -d' ' -f1 | sort | uniq )

echo "Here are users:"
echo "$users"

read -n1 -r -p "Please ctrl-c if that is not correct, any key otherwise..." key

wres_ca_file=../cacerts/wres_ca_x509_cert.pem

if [ -f $wres_ca_file ]
then
    echo "Found the WRES CA file at $wres_ca_file"
else
    echo "Did not find the WRES CA file at $wres_ca_file"
    exit 1
fi

migration_locations=()

for user_name in $users
do
    post_result=$( curl -i -s --cacert $wres_ca_file --data "userName=${user_name}&projectConfig=blah&verb=connecttodb" https://***REMOVED***wres${env_suffix}.***REMOVED***.***REMOVED***/job | tr -d '\r' )
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

    migration_locations+=($job_location)
done

echo "All locations of migration jobs: ${migration_locations[@]}"

jobs_failed=0

for job_location in ${migration_locations[@]}
do
    evaluation_status=""

    while [ "$evaluation_status" != "COMPLETED_REPORTED_SUCCESS" ] \
        && [ "$evaluation_status" != "COMPLETED_REPORTED_FAILURE" ]
    do
        sleep 0.5
        evaluation_status=$( curl --cacert $wres_ca_file $job_location/status -s | tr -d '\r' )
    done

    if [ "$evaluation_status" == "COMPLETED_REPORTED_SUCCESS" ]
    then
        echo "Migration succeeded for $job_location"
    else
        echo "MIGRATION FAILED FOR $job_location"
        ((jobs_failed++))
    fi
done

if [ "$jobs_failed" -ne "0" ]
then
    echo "There were $jobs_failed FAILED MIGRATIONS! See above..."
    exit $jobs_failed
fi

exit 0
