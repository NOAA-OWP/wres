#!/bin/bash

# This installs a system testing .zip under /wres_share/releases and sets the systests
# symbolic link to point to it.  That link always points to the most recently installed
# version.  This used to do more, such as removing evaluation outputs from the previous
# systests, but those tasks are now handled elsewhere.

cd /wres_share/releases
unzip ~/$1.zip

# Note that we used to remove the evaluation outputs here.  Instead, that is now handled
# through a find command using -mtime inside of the top level installBuilt.bash script.

#Remove the existing link and point to the newly installed revision.
rm systests
chmod 775 $1
ln -sv $1 systests


