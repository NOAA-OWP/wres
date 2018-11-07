# Current working directory should be same as script, it is up to the caller
# to ensure that this is true.

setwd( "." )

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

# Run an evaluation using WRES

# Set general http library option of verbose mode
crul::set_opts( verbose = TRUE )

# Set WRES-specific Certificate Authority for all connections and requests
crul::set_opts( capath = "cacerts/" )

# Environment could be -dev or -ti (or perhaps the empty string in future)
wres_env_suffix <- "-dev"

wres_url <- paste( "https://***REMOVED***wres",
                   wres_env_suffix,
                   ".***REMOVED***.***REMOVED***",
                   sep = "" )

# Create a client that prefers "text/plain" but allows other response types.
wres_client <- crul::HttpClient$new( url = wres_url,
                                     headers = list( Accept = "text/plain,*/*" )
)

user_name <- "jesse"
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

# Create an evaluation job
post_response <- wres_client$post( path = "job",
                                   query = list( userName = user_name,
                                                 projectConfig = evaluation ),
                                   encode = "form" )

print( post_response$response_headers )
evaluation_location <- post_response$response_headers$location

if ( post_response$status_code == 201 )
{
    print( "Successfully posted a WRES evaluation job." )
    print( evaluation_location )
} else
{
    print( "Failed to post a job." )
}

# Maybe there is a better way, but for now, cut out the path from the absolute
# location returned by the POST response.
location_url_splitted <- strsplit( evaluation_location, "/" )

# 1 is https, 2 is blank, 3 is nwcal... 4 and following are the path
location_path_only <- paste( location_url_splitted[[1]][4],
                             location_url_splitted[[1]][5],
                             sep = "/" )

# To get a leading /, include an empty string when pasting this together:
output_path <- paste( "", location_path_only, "output", sep = "/" )

print( "Output path:" )
print( output_path )

# To use readLines to read the list of available outputs, we need a temp file
tempFile <- tempfile()

# GET the list of outputs into tempFile, then read the list of outputs
outputResponse <- wres_client$get( path = output_path, disk = tempFile )
availableOutputs <- readLines( tempFile )
#unlink( tempFile )

print( "Available outputs:" )
print( availableOutputs )

# Do something with the output, such as making a plot
for ( availableOutput in availableOutputs )
{
    # The key point is here: read.table can be called with a URL as an argument.
    # There is no need to save data to disk, we read it straight into a data frame.
    wresDataTable <- read.table( paste( evaluation_location,
                                        "output",
                                        availableFile,
                                        sep = "/" ),
                                 header = TRUE,
                                 sep = "," )
    # Do something with the data here. Print it. Plot it.
    print( wresDataTable )
    plot( wresDataTable$LATEST.LEAD.TIME.IN.SECONDS..MEAN.OVER.PAST.3600.SECONDS,
          wresDataTable$MEAN.ERROR.All.data,
          type = "b" )
    Sys.sleep( 2 )
}

# Clean up after ourselves by deleting output (whether successful or not)
delete_response <- wres_client$delete( path = output_path,
                                       encode = "text" )

if ( delete_response$status_code == 200 )
{
    print( "Successfully cleaned up." )
} else
{
    print( "Failed to clean up." )
}
