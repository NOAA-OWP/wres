#!/usr/bin/env Rscript

# This incomplete R program is provided as-is as a guide to the WRES HTTP
# Application Programming Interface. It is expected that a more competent R
# programmer will improve this program prior to use.
# The examples shown here are to indicate what is important about the various
# requests made to the WRES HTTP API, such as methods, headers, and so forth,
# as well as the resource tree modeling WRES jobs.

# Current working directory should be same as script, it is up to the caller
# to ensure that this is true.

setwd( "." )

# Begin reproducible R library section.

# First, specify a location for local libraries
local_libraries <- "local_libraries"
dir.create( file.path( local_libraries ), showWarnings = FALSE )

# Second, set the location for local libraries on .libPaths()
.libPaths( new = local_libraries )

# Third, for within-R-program dependency management, we get checkpoint.
# For reproducibility, we specify the version of checkpoint explicitly.

# Expect 0.4.5 src tar.gz to have particular sha256sum
checkpoint_src_sha256sum <- "8c8b8dcf1456fd6a6f92a624e637280e6be0756f26d771401332dbba51a68656"
checkpoint_src <- "https://cran.r-project.org/src/contrib/checkpoint_0.4.5.tar.gz"

# TODO: verify sha256sum, only try to install if package not already installed
# Important: specify lib to be local_libraries for portability/reproducibility
install.packages( checkpoint_src,
                  repos = NULL,
                  type = "source",
                  lib = local_libraries )

# Checkpoint expects a directory within which to do its work, set when calling
# checkpoint, every time
checkpoint_dir <- ".checkpoint"
dir.create( file.path( checkpoint_dir ), showWarnings = FALSE )

library( checkpoint )

# At this point, we have bootstrapped the ability to specify a date of versions
# of the software we actually care about and to get the versions along
# with that exact version's dependencies (well, at least dependencies based on
# a snapshot of CRAN at a particular time, see checkpoint/MRAN's explanation)

# Pick a midnight-UTC date of MRAN repo which has the versions we want, also
# specify the exact R version we are expected to be running.
checkpoint( "2018-11-04", checkpointLocation = getwd(), R.version = "3.5.1" )

# Finally, proceed as if we already have installed packages. Checkpoint will
# look ahead to these "library" declarations to get/compile what is needful.

# Due to checkpoint, the crul version will be the same for everyone running this
library( crul )

# End reproducible R section

# Run an evaluation using WRES

# First, create an evaluation project configuration, store it in evaluation
evaluation <- '<?xml version="1.0" encoding="UTF-8"?>
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
        <desiredTimeScale>
            <function>mean</function>
            <period>1</period>
            <unit>hours</unit>
        </desiredTimeScale>
    </pair>

    <metrics>
        <metric><name>mean error</name></metric>
        <metric><name>sample size</name></metric>
    </metrics>

    <outputs>
        <destination type="numeric" />
    </outputs>

</project>
'

# We now should have an evaluation project configuration in a variable, show it:
print( "Here is the WRES evaluation project configuration:" )
cat( evaluation )

# There is a -dev environment (WRES Test Platform) and a -ti environment (WRES
# Deployment Platform), therefore we need to specify which one we wish to use.
# Change the following variable to -ti if you are using this program in -ti.
env_suffix <- "-dev"

print( paste( "We are using the", env_suffix, "environment in this example." ) )

# You must edit this user_name variable to be your first name or pass as arg.
user_name <- "i_need_to_edit_this_user_name_to_be_my_first_name"

# Use first command line argument as name, otherwise a 404 will occur.
args <- commandArgs( trailingOnly = TRUE )

if ( length( args ) >= 1 )
{
    print( paste( "Setting user_name to", args[1] ) )
    user_name <- args[1]
}

# Set general http library option of verbose mode, will display HTTP headers.
crul::set_opts( verbose = TRUE )

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

wres_ca_directory <- "cacerts"
wres_ca_file <- paste( wres_ca_directory, "wres_ca_x509_cert.pem", sep = "/" )

if ( file.exists( wres_ca_file ) )
{
    print( paste( "Found the WRES CA file at", wres_ca_file ) )
} else
{
    print( paste( "Did not find the WRES CA file at", wres_ca_file ) )
    quit( status = 1 )
}

# Set WRES-specific Certificate Authority for all connections and requests made
# when using the crul http library.
crul::set_opts( capath = "cacerts/" )

wres_url <- paste( "https://***REMOVED***wres",
                   env_suffix,
                   ".***REMOVED***.***REMOVED***",
                   sep = "" )

# Create a client that prefers "text/plain" but allows other response types.
wres_client <- crul::HttpClient$new( url = wres_url,
                                     headers = list( Accept = "text/plain,*/*" )
)


# Second, POST the evaluation project configuration to the WRES.
# "POST" is a standard HTTP verb, see more about the various HTTP verbs here:
# https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html or
# https://tools.ietf.org/html/rfc2616#section-9.5 or
# https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol#Request_methods
# By POSTing an evaluation project configuration, we are requesting the
# creation of a resource (in this case, an evaluation job) on the server.
# The server will create the resource and we can GET information about the job.
# We furthermore want to save the server's response to the POST, because it
# contains the name of the created resource in its response. We use post_response.
post_response <- wres_client$post( path = "job",
                                   query = list( userName = user_name,
                                                 projectConfig = evaluation ),
                                   encode = "form" )

print( "The response from the server was:" )
print( post_response$response_headers )

# More details about above follow. The $post method causes a POST to be performed
# rather than GET. The query variable contains the parameters and values to POST.

# We check whether the POST was successful by looking for HTTP code 200 or
# 201, something in the 2xx series.
# See https://en.wikipedia.org/wiki/List_of_HTTP_status_codes#2xx_Success for
# information on HTTP response codes.

if ( post_response$status_code == 201 || post_response$status_code == 200 )
{
    print( paste( "The response code was successful:", post_response$status_code ))
} else
{
    print( "The response code was NOT 201 nor 200, failed to create, exiting..." )
    quit( status = 2 )
}

# At this point, we think the response code was successful, so we continue
# by looking for the Location of the uri that was created, it is in an http
# header called Location.

evaluation_location <- post_response$response_headers$location
    
print( paste( "The location of the resource created by server was",
              evaluation_location ) )

# Maybe there is a better way, but for now, cut out the path from the absolute
# location returned by the POST response. This seems specific to crul.
location_url_splitted <- strsplit( evaluation_location, "/" )

# 1 is https, 2 is blank, 3 is nwcal... 4 and following are the path
location_path_only <- paste( location_url_splitted[[1]][4],
                             location_url_splitted[[1]][5],
                             sep = "/" )

# Third, poll the job status with GET requests until the job status changes
# to be COMPLETED_REPORTED_SUCCESS or COMPLETED_REPORTED_FAILURE

evaluation_status <- ""

while ( ! evaluation_status %in% c( "COMPLETED_REPORTED_SUCCESS",
                                    "COMPLETED_REPORTED_FAILURE" ) )
{
    # Pause for two seconds before asking the server for status.
    Sys.sleep( 2 )
    status_response <- wres_client$get( path = paste( location_path_only,
                                                     "status",
                                                     sep = "/" ) )
    evaluation_status <- status_response$parse()
}

# At this point we think the job is finished. If it was a success we can get
# output data. If it was not a success, there should be no output data but
# just in case, we will clean up anyway. But we won't try to GET any.

# To get a leading /, include an empty string when pasting this together:
output_path <- paste( "", location_path_only, "output", sep = "/" )

# Make sure our output URL looks correct:
print( "Output path:" )
print( output_path )

# To use readLines to read the list of available outputs, we need a temp file
tempFile <- tempfile()

if ( evaluation_status == "COMPLETED_REPORTED_SUCCESS" )
{
    # When looking for output data, there are two representations of the list
    # of output data. One is text/html, one is text/plain. The client may ask
    # for one or the other or give a priority list to the server in the Accept
    # HTTP header. For more information on the Accept header, see 
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
    # With crul, we already set text/plain in the Accept header for all requests.
    # GET the list of outputs into tempFile, then read the list of outputs
    outputResponse <- wres_client$get( path = output_path, disk = tempFile )
    availableOutputs <- readLines( tempFile )
    unlink( tempFile )

    print( "Found these output resources available to GET:" )
    print( availableOutputs )


    for ( availableOutput in availableOutputs )
    {
        # The key is here: read.table can be called with a URL as an argument.
        # There is no need to save data to disk, we read it into a data frame.
        wresDataTable <- read.table( paste( evaluation_location,
                                            "output",
                                            availableOutput,
                                            sep = "/" ),
                                    header = TRUE,
                                    sep = "," )
        # Do something with the data here. Print it. Plot it.
        print( wresDataTable )
        plot( wresDataTable$LATEST.LEAD.TIME.IN.SECONDS..MEAN.OVER.PAST.3600.SECONDS,
             wresDataTable$MEAN.ERROR.All.data,
             type = "b" )
    }
} else
{
    print( "Evaluation failed, not attempting to GET output data." )
}

# After completing the evaluation, and retrieving any data from it, it is
# important to clean up resources created by the evaluation.
# Therefore we DELETE the output data.
delete_response <- wres_client$delete( path = output_path,
                                       encode = "text" )

if ( delete_response$status_code == 200 )
{
    print( "Successfully cleaned up." )
} else
{
    print( "Failed to clean up." )
}

print( "" )
print( "Congratulations! You successfully used the WRES HTTP API to" )
print( "1. run an evaluation," )
print( "2. process the results, and" )
print( "3. clean up." )

quit( status = 0 )
