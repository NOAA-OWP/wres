
#
# Untars the package implied by the argument, $1.tgz, It then removes outputs from the 
# previous systests directory and points the symbolic link to this new directory.
# Only argument must be the name of the .zip file without .zip extension. That file is 
# assumed to be in the wres-cron home directory.  
#
cd /wres_share/releases
unzip ~/$1.zip

# This used to be necessary, but was commented out. Not sure if it will need to be resurrected.
#cp install_scripts/config_files/wresconfig.xml $1/lib/conf
#cp install_scripts/config_files/logback.xml $1/lib/conf

echo "The test outputs, .../systests/outputs/wres_evaluation_*, take up a lot of space, so we are removing them."
ls -d systests/outputs/wres_evaluation_*
rm -rf systests/outputs

#Remove the existing link and point to the newly installed revision.
rm systests
chmod 775 $1
ln -sv $1 systests


