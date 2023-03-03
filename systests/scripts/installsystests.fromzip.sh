
#
# Untars the package implied by the argument, $1.tgz, It then removes outputs from the 
# previous systests directory and points the symbolic link to this new directory.
# Only argument must be the name of the .zip file without .zip extension. That file is 
# assumed to be in the wres-cron home directory.  
#
cd /wres_share/releases
unzip ~/$1.zip

# Note that we used to remove the evaluation outputs here.  Instead, that is now handled
# through a find command using -mtime inside of the top level installBuilt.bash script.

#Remove the existing link and point to the newly installed revision.
rm systests
chmod 775 $1
ln -sv $1 systests


