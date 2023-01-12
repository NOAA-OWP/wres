#!/bin/bash

# This bash program is intended for use by admins of a WRES cluster.
# This script runs a clean database on the single db used in an environment.
# It then calls switchdatabase for that COWRES cluster.

# One argument is expected, currently: the target database.
if [ "$#" -ne 3 ] && [ "$#" -ne 4 ] ; then
    echo "Illegal number of arguments provided to clean and switch."
    echo "The following arguments are required: <target db> <target host> <cowres url> [<switch true or false>]"
    echo "Pass in 'active' or an empty string for the db or host to use the active database."
    echo "Pass in false for the optional switch argument if you don't want to switch databases."
    exit 1
fi
target_db=$1
target_host=$2
cowres_url=$3
switchflag=true
if [ "$#" -eq 4 ] ; then
    echo "Switch flag specified at command line: $4"
    switchflag=$4
fi

# If either the target db or host are "active", then empty it out.
if [ $target_db == "active" ]; then
    target_db=""
fi
if [ $target_host == "active" ]; then
    target_host=""
fi

echo "CLEAN AND SWITCH EXECUTING..."
echo "User specified target db $target_db, target host $target_host, and COWRES url $cowres_url."

wres_ca_file=dod_root_ca_3_expires_2029-12.pem

if [ -f $wres_ca_file ]
then
    echo "Found the WRES CA file at $wres_ca_file"
else
    echo "Did not find the WRES CA file at $wres_ca_file"
    exit 1
fi

# First, attempt the clean of the target database
echo "Posting the clean for $target_db on host $target_host to to COWRES URL $cowres_url ..."
post_result=$( curl -i -s --cacert $wres_ca_file --data "additionalArguments=${target_host}&additionalArguments=&additionalArguments=${target_db}&adminToken="***REMOVED***"&verb=cleandatabase" $cowres_url/job | tr -d '\r' )
post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )
echo "The last status code in the response was $post_result_http_code"

# Did the request post successfully?
if [ "$post_result_http_code" -eq "201" ] || [ "$post_result_http_code" -eq "200" ]
then
    echo "The clean request was successfully posted with result $post_result_http_code."
else
    echo "The response code was NOT 201 nor 200, so the clean job failed to create."
    echo "The complete HTTP response:"
    echo ""
    echo $post_result
    echo ""
    echo "Exiting..."
    exit 2
fi

# Parse and log the job location.
job_location=$( echo -n "$post_result" | grep Location | cut -d' ' -f2 )
echo "The location of the resource created by server was $job_location"

evaluation_status=""

# Loop every second until we have a job result.
while [ "$evaluation_status" != "COMPLETED_REPORTED_SUCCESS" ] \
    && [ "$evaluation_status" != "COMPLETED_REPORTED_FAILURE" ]
do
    sleep 1.0
    evaluation_status=$( curl --cacert $wres_ca_file $job_location/status -s | tr -d '\r' )
    echo "The evaluation status obtained is, $evaluation_status."
    if [ "$evaluation_status" != "COMPLETED_REPORTED_SUCCESS" ] \
       && [ "$evaluation_status" != "COMPLETED_REPORTED_FAILURE" ] \
       && [ "$evaluation_status" != "IN_QUEUE" ] \
       && [ "$evaluation_status" != "IN_PROGRESS" ]  
    then
        echo "Obtained unexpected response when requesting status of clean job."
        echo "The job location is $job_location."
        echo "The response obtained was:"
        echo ""
        echo $evaluation_status
        echo ""
        echo "Exiting..."
        exit 5
    fi  
done

# On success, continue.  On failure, exit.
if [ "$evaluation_status" == "COMPLETED_REPORTED_SUCCESS" ]
then
    echo "Clean succeeded for $job_location"
else
    echo "CLEAN FAILED FOR $job_location"
    echo "Since the clean failed, the database will not be switched. Erroring out."
    echo ""
    exit 3
fi

# Don't switch the database if the host and database are active/empty.
if [ "$switchflag" != "true" ]
then
    echo "Switch flag, $switchflag, indicates database is not to be switched.  Exiting."
    exit 0
fi


# So far so good.  Try to switch the database.
echo "Switching to the target database ${target_db} on host $target_host for COWRES url $cowres_url ..."
post_result=$( curl -i -s --cacert $wres_ca_file --data "additionalArguments=${target_host}&additionalArguments=&additionalArguments=${target_db}&adminToken="***REMOVED***"&verb=switchdatabase" $cowres_url/job | tr -d '\r' )
post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 ) 
echo "The last status code in the switchdatabase response was $post_result_http_code" 
if [ "$post_result_http_code" -eq "201" ] || [ "$post_result_http_code" -eq "200" ]
then 
    echo "Successfully switched to the database ${target_db} on host ${target_host}." 
else 
    echo "Failed to switch to the database ${target_db} on host ${target_host}." 
    echo "Complete HTTP response:" 
    echo "" 
    echo $post_result 
    echo ""
    echo "Exiting..."
    exit 4 
fi 

# A good run results in a 0 exit code.
echo "Completed clean-and-switch of database."
exit 0
