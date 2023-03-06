#!/bin/bash

#
# Unpackages the release and set the latest symbolic link to this release.
# This used to do more, like copying over WRES configuration files after the install, bt we
# now use default files delivered with the release.  
#
cd /wres_share/releases
#tar -zxvf ~/$1.tgz
unzip ~/$1.zip

# DO NOT DO per ticket 64737
#  cp install_scripts/config_files/wresconfig.xml $1/lib/conf

# I'm removing the following line, because I want to use the release .zip verison of the file.
# cp install_scripts/config_files/logback.xml $1/lib/conf

# Set the latest symbolic link.
rm latest
chmod 775 $1
ln -s $1 latest


