# Restore the value "16.0" in forecast data
sed -i 's/9001\.0/16.0/g' ../smalldata/1985043014_DRRC2FAKE1_forecast.xml

# That sed is an UNIX/LINUX command, which is unusable for the Windows's user
# will make a general format to implement that sed command for Pythod, Java, Perl, etc.

# File=smalldata/1985043014_DRRC2FAKE1_forecast.xml
# Search=9001.0
# Replace=16.0
# g -- is global/all. Otherwise, put down the line number ###
# Line=g
