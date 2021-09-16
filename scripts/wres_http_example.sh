#!/bin/bash

# This incomplete bash program is provided as-is as a guide to the Central OWP
# WRES HTTP Application Programming Interface. It is expected that a more
# appropriate programming language such as Python or R will be used instead
# bash. The examples shown here are to indicate what is important about the
# various requests made to the WRES HTTP API, such as methods, headers, and so
# forth, as well as the resource tree modeling WRES jobs.

# This main function is included solely for organizational purposes, so that
# data facilitating the example can be at the end of this script.
main()
{

# To begin, we must determine whether or not this example run will post data 
# directly to the COWRES API or rely on files the COWRES already has access to
# on its deployment platform.  This flag will indicate the choice; set to 
# "true" for posting data.
    POST_DATA_DIRECTLY="true" 

# Note that the data to be posted is included, in-line, at the bottom of this
# script, being declared within the function define_data_variables.

# ==================================================================
# 0. Identify the host and locate the .pem CA file. 
# ==================================================================

# Identify  the name of the COWRES host to be communcated with. Specify the
# host accordingly, below.
    host=localhost
    echo "We are using the $host environment in this example."
    echo ""

# The WRES HTTP API uses secured HTTP, aka HTTPS, which is a form of TLS aka
# Transport Layer Security, which relies on X.509 certificates for
# authentication. In this case, the server is providing authentication to the
# client, saying "I am so-and-so". For the client to trust the server, the
# client needs to have pre-arranged trust of the certificate that the server
# will send, or trust of the certificate that signed the certificate that the
# server will send. When making client requests to the server, we instruct the
# client to trust a ca for this request explicitly, referencing a file that
# contains the certificate of the server. The file may be retrieved at
#
# https://***REMOVED***/redmine/projects/wres-user-support/wiki/Import_Certificate_Authority_in_Browser_for_Access_to_WRES_Web_Front-End
#
# Be sure to place the file appropriately and modify the file name, below, to
# point to that file.  
    wres_ca_file=cacerts/dod_root_ca_3_expires_2029-12.pem
    if [ -f $wres_ca_file ]
    then
        echo "Found the WRES CA file at $wres_ca_file"
    else
        echo "Did not find the WRES CA file at $wres_ca_file"
        exit 1
    fi
    echo ""


# ==================================================================
# 1.  Prepare the evaluation declaration.
# ==================================================================

# Next, create an evaluation project declartion and store it in project_config.
# The declaration will differ based on the flag, above, since the source tags
# in the inputs will not be included when we are posting data directly.

# If data will be posted directly to the COWRES, use this declaration.
    if [ $POST_DATA_DIRECTLY = "true" ]; 
    then
        read -d '' project_config << EOF
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
            <variable>SQIN</variable>
        </right>
    </inputs>

    <pair>
        <unit>CMS</unit> 
        <feature left="FAKE1" right="FAKE1" />
    </pair>

    <metrics>
        <metric><name>mean error</name></metric>
        <metric><name>sample size</name></metric>
    </metrics>

    <outputs>
        <destination type="numeric" />
    </outputs>

</project>
EOF

# If files will be read directly by the COWRES, specifically those listed in
# source tags, below, then use this declaration.
    else 
        read -d '' project_config << EOF
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
        <feature left="FAKE1" right="FAKE1" />
    </pair>

    <metrics>
        <metric><name>mean error</name></metric>
        <metric><name>sample size</name></metric>
    </metrics>

    <outputs>
        <destination type="numeric" />
    </outputs>

</project>
EOF

    fi

# We now should have an evaluation project declaration in a variable, show it:
    echo "Here is the WRES evaluation project declaration:"
    echo "$project_config"
    echo ""
# The quotes above around $project_config are important to preserve line breaks.


# ==================================================================
# 2. POST the evaluation declaration. 
# ==================================================================

# POST the evaluation project declaration to the WRES.
#
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
#
# When posting data directly to the COWRES API to support the evaluation, 
# a flag must be included indicating that input is to be posted.  Thus, there
# are two possible commands that can be called.  
    if [ $POST_DATA_DIRECTLY = "true" ];
    then
        # Data is to be posted; note the inclusion of "&postInput=true".
        post_result=$( curl -i --cacert $wres_ca_file --data "projectConfig=${project_config}&postInput=true" https://${host}/job | tr -d '\r' )
    else
        # Data is not to be posted.
        post_result=$( curl -i --cacert $wres_ca_file --data "projectConfig=${project_config}" https://${host}/job | tr -d '\r' )
    fi

    echo "The response from the server was:"
    echo "$post_result"
    echo ""

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

    if [ "$post_result_http_code" -eq "201" ] || [ "$post_result_http_code" -eq "200" ]
    then
        echo "The response code was successful: $post_result_http_code"
    else
        echo "The response code was NOT 201 nor 200, failed to create, exiting..."
        exit 2
    fi
    echo ""

# At this point, we think the response code was successful, so we continue
# by looking for the Location of the uri that was created, it is in an http
# header called Location.
    job_location=$( echo -n "$post_result" | grep Location | cut -d' ' -f2 )
    echo "The location of the resource created by server was $job_location"
    echo ""

# ==================================================================
# 3.  If necessary, post the evaluation data.
# ==================================================================

# Only do this for the data posting version of this script. Otherwise, skip
# to Step 4, below.
    if [ $POST_DATA_DIRECTLY = "true" ];
    then
        echo "This evaluation requires posting data to support it. Posting that data."

# Begin by polling the status of the evaluation.  The status, at this point
# should be AWAITING_POSTS_OF_DATA. If not, something has gone wrong.
        evaluation_status=$( curl --cacert $wres_ca_file $job_location/status | tr -d '\r' )
        if [ "$evaluation_status" != "AWAITING_POSTS_OF_DATA" ];
        then 
            echo "The evaluation status is not AWAITING_POSTS_OF_DATA, which is expected."
            echo "Instead, it is $evaluation_status.  The status can be found at"
            echo "$job_location/status"
            echo "Exiting..."
            exit 2
        fi 

# Post the data to the COWRES.  If a failure is encountered, this example
# will continue with the evaluation to ensure the evaluation job is not left
# in a hanging state of AWAITING_POSTS_OF_DATA.  However, the evaluation will
# either fail or have bad results.  In practice, if it fails, then simply post
# the input done HTTP request, below, and let the evaluation fail.

# First, post observation data on the "left" side of the evaluation.
# Note that "input/left" is appended to the $job_location.
        post_result=$(curl -i --cacert $wres_ca_file --form-string data="${observation_data}" $job_location/input/left | tr -d '\r')
        post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )
        if [ "$post_result_http_code" -eq "201" ] || [ "$post_result_http_code" -eq "200" ]
        then
            echo "The response code to posting left data was successful: $post_result_http_code"
        else
            echo "The response code to posting left data was a failure: $post_result_http_code"
            echo "Continuing with the evaluation to ensure it is not left in a hanging state."
        fi

# Now post the forecasts to the "right" side of the evaluation, those
# forecasts being defined in a bash array (there are three sets of data).  
# Note that "input/right" is appended to the $job_location.
        for fcst_data_var in "${forecast_data[@]}"; 
        do
            echo "Posting forecast file number $i..."
            post_result=$(curl -i --cacert $wres_ca_file --form-string data="${fcst_data_var}" $job_location/input/right | tr -d '\r')
            post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )
            if [ "$post_result_http_code" -eq "201" ] || [ "$post_result_http_code" -eq "200" ]
            then
                echo "The response code to posting left data was successful: $post_result_http_code"
            else
                echo "The response code to posting left data was a failure: $post_result_http_code"
                echo "Continuing with the evaluation to ensure it is not left in a hanging state."
            fi
        done 

# The data has been POSTed.  Now we must tell the COWRES that we are 
# done posting input.  This is done by POSTing "postInputDone=true" to 
# $job_location/input.
        post_result=$(curl -i --cacert $wres_ca_file -d postInputDone=true $job_location/input)
        post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )
        if [ "$post_result_http_code" -eq "201" ] || [ "$post_result_http_code" -eq "200" ]
        then
            echo "The response code for posting input-done was successful: $post_result_http_code"
        else
            echo "The response code for posting input-done was NOT 201 nor 200, failed to create, exiting..."
            echo "Complete response: $post_result"
            exit 2
        fi

        echo "Data posted to the COWRES; failures have been reported, above."
        echo "Continuing with the evaluation."
    fi

# ==================================================================
# 4. Monitor the job for success or failure. 
# ==================================================================

# Poll the job status with GET requests until the job status changes to be
# COMPLETED_REPORTED_SUCCESS or COMPLETED_REPORTED_FAILURE
    echo "Monitoring the evaluation job for success or failure."
    evaluation_status=""
    while [ "$evaluation_status" != "COMPLETED_REPORTED_SUCCESS" ] \
            && [ "$evaluation_status" != "COMPLETED_REPORTED_FAILURE" ] \
            && [ "$evaluation_status" != "NOT_FOUND" ]
    do
        # Pause for two seconds before asking the server for status.
        sleep 2
        evaluation_status=$( curl --cacert $wres_ca_file $job_location/status | tr -d '\r' )
        echo "Status found: $evaluation_status"
    done

# ==================================================================
# 5. On success, obtain the evaluation outputs. 
# ==================================================================

# At this point we think the job is finished. If it was a success we can GET 
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
            some_output=$( curl --cacert $wres_ca_file $job_location/output/$output | tr -d '\r' )

            # It is at this point in the script that output from WRES may be
            # processed. Here we print the output in its binary form using xxd.
            # A more realistic action might be to create a plot using the data.
            echo "Here is output from $output:"
            echo -n "$some_output" | xxd
        done
    elif [ "$evaluation_status" == "NOT_FOUND" ]
    then
        echo "Evaluation not found, WRES HTTP API is mildly amnesiac."
    else
        echo "Evaluation failed, not attempting to GET output data."
    fi

# ==================================================================
# 6. Clean up after yourself. 
# ==================================================================

# After completing the evaluation, and retrieving any data from it, it is
# important to clean up resources created by the evaluation.
# Therefore we DELETE the output data.
    curl -v -X DELETE --cacert $wres_ca_file $job_location/output

    echo ""
    echo "Congratulations! You successfully used the WRES HTTP API to"
    echo "run an evaluation,"
    echo "process the results, and"
    echo "clean up."

# Close the main function.  Again, main was only used to better
# organize this script.  
}


# This function is used to define the data for the evaluation.  All
# of the data is "fake", meaning it is manufactured to support this
# basic example.  The format is PI-timeseries XML, which the WRES 
# detects upon ingesting the data.  Four data-containing variables
# are defined for use when posting data directly: observations
# to be posted on the "left" side of the evaluation, and three
# forecasts to be posted on the "right" side. The three forecasts
# are then placed in a bash array for easy looping in the script, 
# above.
#
# Alternatively, the data can be stored in files, and the curl
# calls above modified to use "-F data=@filename", but that is not
# necessary when posting data directly to the COWRES.
define_data_variables()
{
    # DRRC2QINE_FAKE_19850430.xml
    read -d '' observation_data << EOF
<?xml version="1.0" encoding="UTF-8"?>
<TimeSeries xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" version="1.2">
    <timeZone>0.0</timeZone>
    <series>
        <header>
            <type>instantaneous</type>
            <locationId>FAKE1</locationId>
            <parameterId>QINE</parameterId>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="1985-04-30" time="01:00:00"/>
            <endDate date="1985-04-30" time="23:00:00"/>
            <missVal>-999.0</missVal>
            <stationName>DOLORES - RICO, BLO</stationName>
            <units>CFS</units>
        </header>
        <event date="1985-04-30" time="01:00:00" value="100.0" flag="0"/>
        <event date="1985-04-30" time="02:00:00" value="150.0" flag="0"/>
        <event date="1985-04-30" time="03:00:00" value="200.0" flag="0"/>
        <event date="1985-04-30" time="04:00:00" value="250.0" flag="0"/>
        <event date="1985-04-30" time="05:00:00" value="300.0" flag="0"/>
        <event date="1985-04-30" time="06:00:00" value="350.0" flag="0"/>
        <event date="1985-04-30" time="07:00:00" value="400.0" flag="0"/>
        <event date="1985-04-30" time="08:00:00" value="450.0" flag="0"/>
        <event date="1985-04-30" time="09:00:00" value="500.0" flag="0"/>
        <event date="1985-04-30" time="10:00:00" value="550.0" flag="0"/>
        <event date="1985-04-30" time="11:00:00" value="600.0" flag="0"/>
        <event date="1985-04-30" time="12:00:00" value="650.0" flag="0"/>
        <event date="1985-04-30" time="13:00:00" value="700.0" flag="0"/>
        <event date="1985-04-30" time="14:00:00" value="750.0" flag="0"/>
        <event date="1985-04-30" time="15:00:00" value="800.0" flag="0"/>
        <event date="1985-04-30" time="16:00:00" value="850.0" flag="0"/>
        <event date="1985-04-30" time="17:00:00" value="900.0" flag="0"/>
        <event date="1985-04-30" time="18:00:00" value="950.0" flag="0"/>
        <event date="1985-04-30" time="19:00:00" value="1000.0" flag="0"/>
        <event date="1985-04-30" time="20:00:00" value="1050.0" flag="0"/>
        <event date="1985-04-30" time="21:00:00" value="1100.0" flag="0"/>
        <event date="1985-04-30" time="22:00:00" value="1150.0" flag="0"/>
        <event date="1985-04-30" time="23:00:00" value="1200.0" flag="0"/>
    </series>
</TimeSeries>
EOF

    # 1985043012_DRRC2FAKE1_forecast.xml
    read -d '' forecast_data_1 << EOF
<?xml version="1.0" encoding="UTF-8"?>
<TimeSeries xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" version="1.5">
    <timeZone>0.0</timeZone>
    <series>
        <header>
            <type>instantaneous</type>
            <locationId>FAKE1</locationId>
            <parameterId>SQIN</parameterId>
            <qualifierId>SIM1</qualifierId>
            <ensembleId>HEFSENSPOST</ensembleId>
            <ensembleMemberIndex>1961</ensembleMemberIndex>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="1985-04-30" time="13:00:00"/>
            <endDate date="1985-04-30" time="18:00:00"/>
            <forecastDate date="1985-04-30" time="12:00:00"/>
            <missVal>-999</missVal>
            <stationName>DOLORES - RICO, BLO</stationName>
            <units>CMS</units>
            <creationDate>2017-09-22</creationDate>
            <creationTime>17:20:00</creationTime>
        </header>
        <event date="1985-04-30" time="13:00:00" value="10.0" flag="0"/>
        <event date="1985-04-30" time="14:00:00" value="20.0" flag="0"/>
        <event date="1985-04-30" time="15:00:00" value="30.0" flag="0"/>
        <event date="1985-04-30" time="16:00:00" value="40.0" flag="0"/>
        <event date="1985-04-30" time="17:00:00" value="50.0" flag="0"/>
        <event date="1985-04-30" time="18:00:00" value="60.0" flag="0"/>
    </series>
    <series>
        <header>
            <type>instantaneous</type>
            <locationId>FAKE1</locationId>
            <parameterId>SQIN</parameterId>
            <qualifierId>SIM1</qualifierId>
            <ensembleId>HEFSENSPOST</ensembleId>
            <ensembleMemberIndex>1962</ensembleMemberIndex>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="1985-04-30" time="13:00:00"/>
            <endDate date="1985-04-30" time="18:00:00"/>
            <forecastDate date="1985-04-30" time="12:00:00"/>
            <missVal>-999</missVal>
            <stationName>DOLORES - RICO, BLO</stationName>
            <units>CMS</units>
            <creationDate>2017-09-22</creationDate>
            <creationTime>17:20:00</creationTime>
        </header>
        <event date="1985-04-30" time="13:00:00" value="11.0" flag="0"/>
        <event date="1985-04-30" time="14:00:00" value="21.0" flag="0"/>
        <event date="1985-04-30" time="15:00:00" value="31.0" flag="0"/>
        <event date="1985-04-30" time="16:00:00" value="41.0" flag="0"/>
        <event date="1985-04-30" time="17:00:00" value="51.0" flag="0"/>
        <event date="1985-04-30" time="18:00:00" value="61.0" flag="0"/>
    </series>
    <series>
        <header>
            <type>instantaneous</type>
            <locationId>FAKE1</locationId>
            <parameterId>SQIN</parameterId>
            <qualifierId>SIM1</qualifierId>
            <ensembleId>HEFSENSPOST</ensembleId>
            <ensembleMemberIndex>1963</ensembleMemberIndex>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="1985-04-30" time="13:00:00"/>
            <endDate date="1985-04-30" time="18:00:00"/>
            <forecastDate date="1985-04-30" time="12:00:00"/>
            <missVal>-999</missVal>
            <stationName>DOLORES - RICO, BLO</stationName>
            <units>CMS</units>
            <creationDate>2017-09-22</creationDate>
            <creationTime>17:20:00</creationTime>
        </header>
        <event date="1985-04-30" time="13:00:00" value="12.0" flag="0"/>
        <event date="1985-04-30" time="14:00:00" value="22.0" flag="0"/>
        <event date="1985-04-30" time="15:00:00" value="32.0" flag="0"/>
        <event date="1985-04-30" time="16:00:00" value="42.0" flag="0"/>
        <event date="1985-04-30" time="17:00:00" value="52.0" flag="0"/>
        <event date="1985-04-30" time="18:00:00" value="62.0" flag="0"/>
    </series>
</TimeSeries>
EOF

    # 1985043013_DRRC2FAKE1_forecast.xml
    read -d '' forecast_data_2 << EOF
<?xml version="1.0" encoding="UTF-8"?>
<TimeSeries xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" version="1.5">
    <timeZone>0.0</timeZone>
    <series>
        <header>
            <type>instantaneous</type>
            <locationId>FAKE1</locationId>
            <parameterId>SQIN</parameterId>
            <qualifierId>SIM1</qualifierId>
            <ensembleId>HEFSENSPOST</ensembleId>
            <ensembleMemberIndex>1961</ensembleMemberIndex>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="1985-04-30" time="14:00:00"/>
            <endDate date="1985-04-30" time="19:00:00"/>
            <forecastDate date="1985-04-30" time="13:00:00"/>
            <missVal>-999</missVal>
            <stationName>DOLORES - RICO, BLO</stationName>
            <units>CMS</units>
            <creationDate>2017-09-22</creationDate>
            <creationTime>17:14:00</creationTime>
        </header>
        <event date="1985-04-30" time="14:00:00" value="13.0" flag="0"/>
        <event date="1985-04-30" time="15:00:00" value="23.0" flag="0"/>
        <event date="1985-04-30" time="16:00:00" value="33.0" flag="0"/>
        <event date="1985-04-30" time="17:00:00" value="43.0" flag="0"/>
        <event date="1985-04-30" time="18:00:00" value="53.0" flag="0"/>
        <event date="1985-04-30" time="19:00:00" value="63.0" flag="0"/>
    </series>
    <series>
        <header>
            <type>instantaneous</type>
            <locationId>FAKE1</locationId>
            <parameterId>SQIN</parameterId>
            <qualifierId>SIM1</qualifierId>
            <ensembleId>HEFSENSPOST</ensembleId>
            <ensembleMemberIndex>1962</ensembleMemberIndex>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="1985-04-30" time="14:00:00"/>
            <endDate date="1985-04-30" time="19:00:00"/>
            <forecastDate date="1985-04-30" time="13:00:00"/>
            <missVal>-999</missVal>
            <stationName>DOLORES - RICO, BLO</stationName>
            <units>CMS</units>
            <creationDate>2017-09-22</creationDate>
            <creationTime>17:14:00</creationTime>
        </header>
        <event date="1985-04-30" time="14:00:00" value="14.0" flag="0"/>
        <event date="1985-04-30" time="15:00:00" value="24.0" flag="0"/>
        <event date="1985-04-30" time="16:00:00" value="34.0" flag="0"/>
        <event date="1985-04-30" time="17:00:00" value="44.0" flag="0"/>
        <event date="1985-04-30" time="18:00:00" value="54.0" flag="0"/>
        <event date="1985-04-30" time="19:00:00" value="64.0" flag="0"/>
    </series>
    <series>
        <header>
            <type>instantaneous</type>
            <locationId>FAKE1</locationId>
            <parameterId>SQIN</parameterId>
            <qualifierId>SIM1</qualifierId>
            <ensembleId>HEFSENSPOST</ensembleId>
            <ensembleMemberIndex>1963</ensembleMemberIndex>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="1985-04-30" time="14:00:00"/>
            <endDate date="1985-04-30" time="19:00:00"/>
            <forecastDate date="1985-04-30" time="13:00:00"/>
            <missVal>-999</missVal>
            <stationName>DOLORES - RICO, BLO</stationName>
            <units>CMS</units>
            <creationDate>2017-09-22</creationDate>
            <creationTime>17:14:00</creationTime>
        </header>
        <event date="1985-04-30" time="14:00:00" value="15.0" flag="0"/>
        <event date="1985-04-30" time="15:00:00" value="25.0" flag="0"/>
        <event date="1985-04-30" time="16:00:00" value="35.0" flag="0"/>
        <event date="1985-04-30" time="17:00:00" value="45.0" flag="0"/>
        <event date="1985-04-30" time="18:00:00" value="55.0" flag="0"/>
        <event date="1985-04-30" time="19:00:00" value="65.0" flag="0"/>
    </series>
</TimeSeries>
EOF

    # 1985043014_DRRC2FAKE1_forecast.xml
    read -d '' forecast_data_3 << EOF
<?xml version="1.0" encoding="UTF-8"?>
<TimeSeries xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" version="1.5">
    <timeZone>0.0</timeZone>
    <series>
        <header>
            <type>instantaneous</type>
            <locationId>FAKE1</locationId>
            <parameterId>SQIN</parameterId>
            <qualifierId>SIM1</qualifierId>
            <ensembleId>HEFSENSPOST</ensembleId>
            <ensembleMemberIndex>1961</ensembleMemberIndex>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="1985-04-30" time="15:00:00"/>
            <endDate date="1985-04-30" time="20:00:00"/>
            <forecastDate date="1985-04-30" time="14:00:00"/>
            <missVal>-999</missVal>
            <stationName>DOLORES - RICO, BLO</stationName>
            <units>CMS</units>
            <creationDate>2017-09-22</creationDate>
            <creationTime>17:20:00</creationTime>
        </header>
        <event date="1985-04-30" time="15:00:00" value="16.0" flag="0"/>
        <event date="1985-04-30" time="16:00:00" value="26.0" flag="0"/>
        <event date="1985-04-30" time="17:00:00" value="36.0" flag="0"/>
        <event date="1985-04-30" time="18:00:00" value="46.0" flag="0"/>
        <event date="1985-04-30" time="19:00:00" value="56.0" flag="0"/>
        <event date="1985-04-30" time="20:00:00" value="66.0" flag="0"/>
    </series>
    <series>
        <header>
            <type>instantaneous</type>
            <locationId>FAKE1</locationId>
            <parameterId>SQIN</parameterId>
            <qualifierId>SIM1</qualifierId>
            <ensembleId>HEFSENSPOST</ensembleId>
            <ensembleMemberIndex>1962</ensembleMemberIndex>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="1985-04-30" time="15:00:00"/>
            <endDate date="1985-04-30" time="20:00:00"/>
            <forecastDate date="1985-04-30" time="14:00:00"/>
            <missVal>-999</missVal>
            <stationName>DOLORES - RICO, BLO</stationName>
            <units>CMS</units>
            <creationDate>2017-09-22</creationDate>
            <creationTime>17:20:00</creationTime>
        </header>
        <event date="1985-04-30" time="15:00:00" value="17.0" flag="0"/>
        <event date="1985-04-30" time="16:00:00" value="27.0" flag="0"/>
        <event date="1985-04-30" time="17:00:00" value="37.0" flag="0"/>
        <event date="1985-04-30" time="18:00:00" value="47.0" flag="0"/>
        <event date="1985-04-30" time="19:00:00" value="57.0" flag="0"/>
        <event date="1985-04-30" time="20:00:00" value="67.0" flag="0"/>
    </series>
    <series>
        <header>
            <type>instantaneous</type>
            <locationId>FAKE1</locationId>
            <parameterId>SQIN</parameterId>
            <qualifierId>SIM1</qualifierId>
            <ensembleId>HEFSENSPOST</ensembleId>
            <ensembleMemberIndex>1963</ensembleMemberIndex>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="1985-04-30" time="15:00:00"/>
            <endDate date="1985-04-30" time="20:00:00"/>
            <forecastDate date="1985-04-30" time="14:00:00"/>
            <missVal>-999</missVal>
            <stationName>DOLORES - RICO, BLO</stationName>
            <units>CMS</units>
            <creationDate>2017-09-22</creationDate>
            <creationTime>17:20:00</creationTime>
        </header>
        <event date="1985-04-30" time="15:00:00" value="18.0" flag="0"/>
        <event date="1985-04-30" time="16:00:00" value="28.0" flag="0"/>
        <event date="1985-04-30" time="17:00:00" value="38.0" flag="0"/>
        <event date="1985-04-30" time="18:00:00" value="48.0" flag="0"/>
        <event date="1985-04-30" time="19:00:00" value="58.0" flag="0"/>
        <event date="1985-04-30" time="20:00:00" value="68.0" flag="0"/>
    </series>
</TimeSeries>
EOF

    forecast_data=( 0 1 2 )
    forecast_data[0]="$forecast_data_1"
    forecast_data[1]="$forecast_data_2"
    forecast_data[2]="$forecast_data_3"
}

# Execute the example, above, just as an illustration.  
define_data_variables
main

exit 0
