
#
# Untars the package implied by the argument, $1.tgz, and then copies the wresconfig.xml and logback.xml from 
# latest to the new release.  This then updates the latest link to point to this new release.
#
cd /wres_share/releases
#tar -zxvf ~/$1.tgz
unzip ~/$1.zip
# DO NOT DUE per ticket 64737... cp install_scripts/config_files/wresconfig.xml $1/lib/conf
# I'm removing the following line, because I want to use the release .zip verison of the file.
#cp install_scripts/config_files/logback.xml $1/lib/conf
rm latest
chmod 775 $1
ln -s $1 latest


