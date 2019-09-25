#!/usr/bin/env python3

import argparse
import os
import time

import requests

# This incomplete python program is provided as-is as a guide to the WRES HTTP
# Application Programming Interface. It is expected that a person will improve
# this program prior to use.
# The examples shown here are to indicate what is important about the various
# requests made to the WRES HTTP API, such as methods, headers, and so forth,
# as well as the resource tree modeling WRES jobs.

# First, create an evaluation project configuration, store it in evaluation
evaluation = '''
<?xml version="1.0" encoding="UTF-8"?>
<project name="scenario50x">

    <inputs>
        <left>
            <type>observations</type>
            <source>/mnt/wres_share/systests/smalldata/DRRC2QINE_FAKE_19850430.xml</source>
            <variable>QINE</variable>
        </left>
        <right>
            <type>ensemble forecasts</type>
            <source>/mnt/wres_share/systests/smalldata/1985043012_DRRC2FAKE1_forecast.xml</source>
            <source>/mnt/wres_share/systests/smalldata/1985043013_DRRC2FAKE1_forecast.xml</source>
            <source>/mnt/wres_share/systests/smalldata/1985043014_DRRC2FAKE1_forecast.xml</source>
            <variable>SQIN</variable>
        </right>
    </inputs>

    <pair>
        <unit>CMS</unit>
        <feature locationId="FAKE1" />
    </pair>

    <metrics>
        <metric><name>mean error</name></metric>
        <metric><name>sample size</name></metric>
    </metrics>

    <outputs>
        <destination type="numeric" />
    </outputs>

</project>
'''

# We now should have an evaluation project configuration in a variable, show it:
print( "Here is the WRES evaluation project configuration:" )
print( evaluation )

# There is a -dev environment (WRES Test Platform) and a -ti environment (WRES
# Deployment Platform), therefore we need to specify which one we wish to use.
# Change the following variable to -ti if you are using this program in -ti.
env_suffix = "-dev"

print( "We are using the " + env_suffix + " environment in this example." )

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

wres_ca_file = "cacerts/wres_ca_x509_cert.pem"

if os.path.exists( wres_ca_file ):
    print( "Found the WRES CA file at " + wres_ca_file )
else:
    print( "Did not find the WRES CA file at" + wres_ca_file )
    exit( 1 )

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

post_result = requests.post( url="https://***REMOVED***wres"+env_suffix+".***REMOVED***.***REMOVED***/job",
                             verify = wres_ca_file,
                             data = { 'projectConfig': evaluation } )

print( "The response from the server was:" )
print( post_result )

# More details about above follow. The "verify" parameter specifies the
# Certificate Authority to trust for this request (see above). The "data"
# parameter contains the map of query parameters and their values. Use of the
# "post" method causes an HTTP POST to be performed.

# We check whether the POST was successful by looking for HTTP code 200 or
# 201, something in the 2xx series.
# See https://en.wikipedia.org/wiki/List_of_HTTP_status_codes#2xx_Success for
# information on HTTP response codes.

print( "The last status code in the response was "
       + str( post_result.status_code ) )

if post_result.status_code == 201 or post_result.status_code == 200:
    print( "The response code was successful: "
           + str( post_result.status_code ) )
else:
    print( "The response code was NOT 201 nor 200, failed to create, exiting..." )
    exit( 2 )

# At this point, we think the response code was successful, so we continue
# by looking for the Location of the uri that was created, it is in an http
# header called Location.

job_location = post_result.headers['Location']
print( "The location of the resource created by server was " + job_location )

# Third, poll the job status with GET requests until the job status changes
# to be COMPLETED_REPORTED_SUCCESS or COMPLETED_REPORTED_FAILURE

evaluation_status=""

while ( evaluation_status != "COMPLETED_REPORTED_SUCCESS"
        and evaluation_status != "COMPLETED_REPORTED_FAILURE"
        and evaluation_status != "NOT_FOUND" ):
    # Pause for two seconds before asking the server for status.
    # If your evaluations take around 2 minutes to 2 hours this is appropriate.
    # If your evaluations take over 2 hours, increase to 20 seconds.
    # If your evaluations take less than 2 minutes, drop to 0.2 seconds.
    time.sleep( 2 )
    evaluation_status = requests.get( url = job_location + "/status",
                                      verify = wres_ca_file
                                    ).text

# At this point we think the job is finished. If it was a success we can get
# output data. If it was not a success, there should be no output data but
# just in case, we will clean up anyway. But we won't try to GET any.

output_data=""

if evaluation_status == "COMPLETED_REPORTED_SUCCESS":
    # When looking for output data, there are two representations of the list
    # of output data. One is text/html, one is text/plain. The client may ask
    # for one or the other or give a priority list to the server in the Accept
    # HTTP header. For more information on the Accept header, see 
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
    # In requests, we use the header arg to specify the Accept header and to
    # set it to text/plain, because WRES text/plain is easier to parse than html
    output_data = requests.get( url = job_location + "/output",
                                verify = wres_ca_file,
                                headers = { 'Accept': 'text/plain' }
                              ).text

    print( "Found these output resources available to GET:" )
    print( output_data )

    for output in output_data.splitlines():
        some_output = requests.get( url = job_location + "/output/" + output,
                                    verify = wres_ca_file )

        # It is at this point in the script that output from WRES may be
        # processed. Here we print the output in its text form if it is text.
        # A more realistic action might be to create a plot using the data.
        if ( some_output.headers['Content-Type'].startswith( 'text' ) ):
             print( "Here is output from " + output + ":" )
             print( some_output.text )
        else:
             print( "Non-text output was returned for " + output )
elif evaluation_status == "NOT_FOUND":
    print( "Evaluation not found, WRES HTTP API is mildly amnesic." )
else:
    print( "Evaluation failed, not attempting to GET output data." )

# After completing the evaluation, and retrieving any data from it, it is
# important to clean up resources created by the evaluation.
# Therefore we DELETE the output data.
requests.delete( url = job_location + "/output",
                 verify = wres_ca_file )

print( "" )
print( "Congratulations! You successfully used the WRES HTTP API to" )
print( "1. run an evaluation," )
print( "2. process the results, and" )
print( "3. clean up." )

exit( 0 )
