# Current working directory should be same as script, it is up to the caller
# to ensure that this is true.

setwd( "." )

# First, specify a location for local libraries
local_libraries <- "local_libraries"
dir.create( file.path( local_libraries ), showWarnings = FALSE )

# Second, set the location for local libraries on .libPaths()
.libPaths( new=local_libraries )

# Third, for within-R-program dependency management, we get checkpoint.
# For reproducibility, we specify the version of checkpoint explicitly.

# Expect 0.4.5 src tar.gz to have particular sha256sum
checkpoint_src_sha256sum <- "8c8b8dcf1456fd6a6f92a624e637280e6be0756f26d771401332dbba51a68656"
checkpoint_src <- "https://cran.r-project.org/src/contrib/checkpoint_0.4.5.tar.gz"

# TODO: verify sha256sum
# Important: specify lib to be local_libraries for portability/reproducibility
install.packages( checkpoint_src, repos=NULL, type="source", lib=local_libraries )


# Checkpoint expects a directory within which to do its work, set when calling
# checkpoint, every time
checkpoint_dir <- ".checkpoint"
dir.create( file.path( checkpoint_dir ), showWarnings=FALSE )

library( checkpoint )

# At this point, we have bootstrapped the ability to specify a date of versions
# of the software we actually care about and to get the versions along
# with that exact version's dependencies (well, at least dependencies based on
# a snapshot of CRAN at a particular time, see checkpoint/MRAN's explanation)

# Pick a midnight-UTC date of MRAN repo which has the versions we want, also
# specify the exact R version we are expected to be running.
checkpoint( "2018-11-04", checkpointLocation=getwd(), R.version="3.5.1" )

# Finally, proceed as if we already have installed packages. Checkpoint will
# look ahead to these "library" declarations to get/compile what is needful.

# Due to checkpoint, the crul version will be the same for everyone running this
library( crul )
