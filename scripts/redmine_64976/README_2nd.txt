This script command is 

./getMultipleDataTypes.bash -t data_type [-h destination_host -d /destination_dir -u remote_loginID -w yyyymmdd[from_yyyymmdd to_yyyymmdd] -s thisScript_dir -V data_version -k yes/no]


The D-store is in  http://***REMOVED***dstore.***REMOVED***.***REMOVED***/
and the data directory in D-store is nwm/{version}/nwm.yyyymmdd; 
by default version is 2.0, default yyyymmdd is today.
If you want to get data files other than today, then you have to use '-w yyyymmdd' or '-w from_yyyymmdd to_yyyymmdd' option.
If you want to get different version, then you need with '-V version' option.

By default the data files destination_host is ***REMOVED***wres-dev02.***REMOVED***.***REMOVED*** , it can be overrided by -h option.
By default the data files destination_dir is /mnt/wres_share/nwmData, it can be overrided by -d option.
By default the remote_loginID is the same as your current host login ID, use -u option to override it if it's different from current host.
The -k option is 'yes', place data files with SSH_KEYS (default). Otherwise, 'no'

Where the '-t data_type' is required, and the data_type are list in D-store: 
http://***REMOVED***dstore.***REMOVED***.***REMOVED***/nwm/{version}/nwm.yyyymmdd/, you specify more than one data types with '-t' option as:

-t "data_type1 data_type2". 
For example:
-t "medium_range short_range"
