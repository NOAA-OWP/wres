#!/bin/bash

# This bash program is intended for use by admins of a WRES cluster.
# This tests the ability of the broker and tasker to handle many evaluations.
# Intended to be run from a directory one level deeper than wres/scripts.

env_suffix=-dev

echo "We are using the $env_suffix environment in this program."

count_simultaneous=2
count_total=10

if [ $count_total -lt $count_simultaneous ]
then
    echo "ERROR: Total jobs (set to $count_total) must exceed simultaneous jobs (set to $count_simultaneous)."
    exit 1
fi

actual_max=$(( count_simultaneous + count_total ))
echo "We will run ${count_simultaneous} evaluations simultaneously, a total of ${count_total} to ${actual_max}."
read -n1 -r -p "Please ctrl-c if that is not correct, any key otherwise..." key

wres_ca_file=../cacerts/wres_ca_x509_cert.pem

if [ -f $wres_ca_file ]
then
    echo "Found the WRES CA file at $wres_ca_file"
else
    echo "Did not find the WRES CA file at $wres_ca_file"
    exit 2
fi

job_number=0
evaluation_locations=()
jobs_in_progress=()

# 1. Submit $count_simultaneous jobs before looking for results.

for (( i=1; i<=$count_simultaneous; i++ ))
do
    job_number=$(( job_number + 1 ))
    echo "Submitting job number $job_number"
    post_result=$( curl -i -s --cacert $wres_ca_file --data "projectConfig=NA&verb=ingest" https://***REMOVED***wres${env_suffix}.***REMOVED***.***REMOVED***/job | tr -d '\r' )
    post_exit_code=$?
    echo "The exit code of curl was $post_exit_code"
    post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )
    echo "The last status code in the response was $post_result_http_code"

    if [ "$post_result_http_code" -eq "201" ] || [ "$post_result_http_code" -eq "200" ]
    then
        echo "The response code was successful for job number $job_number: $post_result_http_code"
    else
        echo "The response code was NOT 201 nor 200, failed to create job number $job_number, exiting..."
        exit 3
    fi

    original_job_location=$( echo -n "$post_result" | grep Location | cut -d' ' -f2 )
    echo "The location of the resource created by server for job number $job_number was $original_job_location"
    evaluation_locations+=($original_job_location)
    jobs_in_progress+=($original_job_location)
done

# 2. Iterate over the $count_simultaneous in-progress jobs looking for a completion.
# 3. For each completed job, submit another one until we have submitted $count_total jobs.

jobs_completed_success=()
jobs_completed_failure=()

while (( $job_number <= $count_total ))
do
    jobs_still_in_progress=()

    for job_location in ${jobs_in_progress[@]}
    do
        evaluation_status=$( curl --cacert $wres_ca_file $job_location/status -s | tr -d '\r' )

        if [ "$evaluation_status" == "NOT_FOUND" ]
        then
            echo "Evaluation $job_location not found! Not good!"
            exit 4
        elif [ "$evaluation_status" != "COMPLETED_REPORTED_SUCCESS" ] \
               && [ "$evaluation_status" != "COMPLETED_REPORTED_FAILURE" ]
        then
            echo "Evaluation still in progress: $job_location"
            jobs_still_in_progress+=($job_location)
        else
            if [ "$evaluation_status" == "COMPLETED_REPORTED_SUCCESS" ]
            then
                echo "Evaluation succeeded for $job_location"
                jobs_completed_success+=($job_location)
            elif [ "$evaluation_status" == "COMPLETED_REPORTED_FAILURE" ]
            then
                echo "EVALUATION FAILED FOR $job_location"
                jobs_completed_failure+=($job_location)
            fi

	    job_number=$(( $job_number + 1 ))
	    echo "Submitting job number $job_number"
	    post_result=$( curl -i -s --cacert $wres_ca_file --data "projectConfig=NA&verb=ingest" https://***REMOVED***wres${env_suffix}.***REMOVED***.***REMOVED***/job | tr -d '\r' )
	    post_exit_code=$?
	    echo "The exit code of curl was $post_exit_code"
	    post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )
	    echo "The last status code in the response was $post_result_http_code"

	    if [ "$post_result_http_code" -eq "201" ] || [ "$post_result_http_code" -eq "200" ]
	    then
                echo "The response code was successful for job number $job_number: $post_result_http_code"
	    else
                echo "The response code was NOT 201 nor 200, failed to create job number $job_number, exiting..."
                exit 5
	    fi
	    new_job_location=$( echo -n "$post_result" | grep Location | cut -d' ' -f2 )
	    echo "The location of the resource created by server for job number $job_number was $new_job_location"
	    evaluation_locations+=($new_job_location)
	    jobs_still_in_progress+=($new_job_location)
        fi
    done

    # Clear out the outer-most jobs marked in progress, update with latest info.
    jobs_in_progress=()
    for job_location_still_in_progress in ${jobs_still_in_progress[@]}
    do
        jobs_in_progress+=($job_location_still_in_progress)
    done
done

# 4. Iterate over the remaining unfinished jobs looking for their completions.

echo "Submitted total of $job_number jobs, now looking for remaining evaluations."

for job_location in ${jobs_in_progress[@]}
do
    evaluation_status=""

    while [ "$evaluation_status" != "COMPLETED_REPORTED_SUCCESS" ] \
           && [ "$evaluation_status" != "COMPLETED_REPORTED_FAILURE" ]
    do
        sleep 0.2
	echo "Looking for job at $job_location"
	evaluation_status=$( curl --cacert $wres_ca_file $job_location/status -s | tr -d '\r' )
    done

    if [ "$evaluation_status" == "COMPLETED_REPORTED_SUCCESS" ]
    then
        echo "Evaluation succeeded for $job_location"
        jobs_completed_success+=($job_location)
    elif [ "$evaluation_status" == "COMPLETED_REPORTED_FAILURE" ]
    then
        echo "EVALUATION FAILED FOR $job_location"
        jobs_completed_failure+=($job_location)
    fi
done

echo "All locations of evaluation jobs: ${evaluation_locations[@]}"
echo "Failed evaluation jobs: ${jobs_completed_failure[@]}"
echo "Successful evaluation jobs: ${jobs_completed_success[@]}"

exit 0
