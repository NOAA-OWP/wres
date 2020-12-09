#!/bin/bash

# This incomplete bash program is provided as-is as a guide to the WRES HTTP
# Application Programming Interface. It is expected that a more appropriate
# programming language such as Python or R will be used instead of bash.
# The examples shown here are to indicate what is important about the various
# requests made to the WRES HTTP API, such as methods, headers, and so forth,
# as well as the resource tree modeling WRES jobs.

# Confirm that one argument is specified. 
if [ $# -ne 2 ]
then
    echo 
    echo "Command line format: "
    echo
    echo "    wres_http_example.sh <user name> <project config file>"
    echo
    echo "    <user name> = First name of user's user name on the Linux VMs."
    echo "    <project config file> = The name of the XML project configuration file for the evaluation to perform."
    echo 
    echo "For example:"
    echo 
    echo "    wres_http_example.sh bob project_config.xml"
    echo 
    exit 1
fi

# Capture the contents of the XML file provided in the variable, project_config. 
project_config=`cat $2`

# We now should have an evaluation project configuration in a variable, show it:
echo "Here is the WRES evaluation project configuration:"
echo "$project_config"

env_suffix=-prod

echo "We are using the $env_suffix environment in this example."

# The WRES HTTP API uses secured HTTP, aka HTTPS, which is a form of TLS aka
# Transport Layer Security, which relies on X.509 certificates for
# authentication. In this case, the server is providing authentication to the
# client, saying "I am so-and-so". For the client to trust the server, the
# client needs to have pre-arranged trust of the certificate that the server
# will send, or trust of the certificate that signed the certificate that the
# server will send. When making client requests to the server, we instruct the
# client to trust a ca for this request explicitly, referencing a file that
# contains the certificate of the server. The file may be retrieved at
# https://***REMOVED***/redmine/projects/wres-user-support/wiki/Import_Certificate_Authority_in_Browser_for_Access_to_WRES_Web_Front-End

wres_ca_file=../wres_ca_x509_cert.pem

if [ -f $wres_ca_file ]
then
    echo "Found the WRES CA file at $wres_ca_file"
else
    echo "Did not find the WRES CA file at $wres_ca_file, please update script."
    exit 1
fi

# You must edit this user_name variable to be your first name or pass as arg.
user_name=i_need_to_edit_this_user_name_to_be_my_first_name

if [ ! -z "$1" ]
then
    user_name=$1
fi


# Second, POST the evaluation project configuration to the WRES.
# "POST" is a standard HTTP verb, see more about the various HTTP verbs here:
# https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html or
# https://tools.ietf.org/html/rfc2616#section-9.5 or
# https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol#Request_methods
# By POSTing an evaluation project configuration, we are requesting the
# creation of a resource (in this case, an evaluation job) on the server.
# The server will create the resource and we can GET information about the job.
# We furthermore want to save the server's response to the POST, because it
# contains the name of the created resource in its response. We use post_result.
# The tr -d is to remove carriage return characters from the response.
post_result=$( curl -i --cacert $wres_ca_file --data "userName=${user_name}&projectConfig=${project_config}" https://***REMOVED***wres${env_suffix}.***REMOVED***.***REMOVED***/job | tr -d '\r' )

echo "The response from the server was:"
echo "$post_result"

# More details about above follow. The -i option of curl causes response headers
# to be returned. The --cacert option of curl specifies the Certificate
# Authority to trust for this request (see above). The --data option of curl
# causes a POST to be performed rather than the default GET, inside which are
# the parameters and values to POST.

# We check whether the POST was successful by looking for HTTP code 200 or
# 201, something in the 2xx series.
# See https://en.wikipedia.org/wiki/List_of_HTTP_status_codes#2xx_Success for
# information on HTTP response codes.

post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )
echo "The last status code in the response was $post_result_http_code"

if [ "$post_result_http_code" -eq "201" ] \
   || [ "$post_result_http_code" -eq "200" ]
then
    echo "The response code was successful: $post_result_http_code"
else
    echo "The response code was NOT 201 nor 200, failed to create, exiting..."
    exit 2
fi

# At this point, we think the response code was successful, so we continue
# by looking for the Location of the uri that was created, it is in an http
# header called Location.

job_location=$( echo -n "$post_result" | grep Location | cut -d' ' -f2 )
echo "The location of the resource created by server was $job_location"

# Third, poll the job status with GET requests until the job status changes
# to be COMPLETED_REPORTED_SUCCESS or COMPLETED_REPORTED_FAILURE

evaluation_status=""

while [ "$evaluation_status" != "COMPLETED_REPORTED_SUCCESS" ] \
      && [ "$evaluation_status" != "COMPLETED_REPORTED_FAILURE" ]
do
    # Pause for two seconds before asking the server for status.
    sleep 2
    evaluation_status=$( curl --cacert $wres_ca_file $job_location/status | tr -d '\r' )
done
echo "The job completed with status, $evaluation_status"

# At this point we think the job is finished. If it was a success we can get
# output data. If it was not a success, there should be no output data but
# just in case, we will clean up anyway. But we won't try to GET any.

output_data=""

if [ "$evaluation_status" == "COMPLETED_REPORTED_SUCCESS" ]
then
    # When looking for output data, there are two representations of the list
    # of output data. One is text/html, one is text/plain. The client may ask
    # for one or the other or give a priority list to the server in the Accept
    # HTTP header. For more information on the Accept header, see 
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
    # In curl, we use the --header option to specify the Accept header and to
    # set it to text/plain, because WRES text/plain is easier to parse than html
    output_data=$( curl --cacert $wres_ca_file --header "Accept: text/plain" $job_location/output | tr -d '\r' )
    echo "Found these output resources available to GET:"
    echo $output_data

    for output in $output_data
    do
        curl --cacert $wres_ca_file $job_location/output/$output > output/$output

        # It is at this point in the script that output from WRES may be
        # processed. Here we print the output in its binary form using xxd.
        # A more realistic action might be to create a plot using the data.
        echo "Here is output from $output:"
        echo -n "$some_output" | xxd
    done
else
    echo "Evaluation failed, not attempting to GET output data."
    error_output=$( curl --cacert $wres_ca_file $job_location/stderr | tr -d '\r' )
    echo "Error output:"
    echo $error_output
fi

# After completing the evaluation, and retrieving any data from it, it is
# important to clean up resources created by the evaluation.
# Therefore we DELETE the output data.
curl -v -X DELETE --cacert $wres_ca_file $job_location/output

echo ""
echo "Congratulations! You successfully used the WRES HTTP API to"
echo "1. run an evaluation,"
echo "2. process the results, and"
echo "3. clean up."
