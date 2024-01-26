It is an user's responsibility to clean up the data files under destination_host/destination_dir/user_datafilePath/ 

Suggest run a cron job in destination_host shows below.

find /destination_dir/user_datafilePath/ -name '*' -mtime +5 -exec rm {} \; # remove all files older tahn 5 days
find . -empty -exec rm -fv {} \;	# remove all empty files
find . -empty -exec rmdir -v --ignore-fail-on-non-empty {} \; # remove all empty directories
