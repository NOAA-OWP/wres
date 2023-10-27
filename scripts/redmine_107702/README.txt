Execution of the wres_caller.py script requires the Python libraries requests
and urllib3. To create your run environment, required only the first time, do
the following:

  $ python -m venv ./venv

To activate your environment and prepare it for executing the script, do the
following:

  $ source venv/bin/activate
  (venv) $ pip install requests
  (venv) $ pip install urllib3==1.26.6

You can then run the script with a -h argument to list options:

  (venv) $ python [path]/wres_caller.py -h

Note that the install of a specific version of urllib3 may not be required. To
test if it is required, run the script with -h before installing urllib3. If it
works, then you should be good to go. Also note that the same @venv@ can be 
employed for future executions of the script without needing to install the
packages again.

A typical execution of the script will look like this:

  python wres_caller.py -u [WRES hostname] -c dod_root_ca_3_expires_2029-12.pem evaluation.yml

Both the host and the name of the CA files can be specified by environment 
variable. For more information read the options listed with the -h argument.
