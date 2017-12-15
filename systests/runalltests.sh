# Executes all standard system tests scenarios; i.e., scenario*.  It does not run the manualScenario* tests.
# Run this within the Git cloned directory .../wres/systests, where the runtest.sh script is found.

# Read the options, which is currently only one: -l to indicate a run of latest. 
latest=0
while getopts "l" option; do
     case "${option}"
     in
         l) latest=1;
     esac
done
shift $((OPTIND -1))

# Always cd to the directory containing this script to execute!
if [ ! -f runtest.sh ]; then
    echo "You are not in the directory that contains this run script.  Aborting..."
    exit 1;
fi

# Handle a latest run request by clearing out the working rls
if [[ "$latest" == 1 ]]; then
    echo "Removing the working release link, ~/wresTestScriptWorkingRelease, if it exists so that the latest release of WRES is used."
    rm -f ~/wresTestScriptWorkingRelease
fi
 
# Identify the log file name.
logFileName="wresSysTestResults_$(date +"%Y%m%d_%H%M%S").txt"

# Prep the log file.
touch $logFileName
echo "System test results for scenarios:" > $logFileName
ls -d scenario* >> $logFileName

echo "System test results for scenarios:" 
ls -d scenario*

# Run and time the run.
startsec=$(date +%s)

./runtest.sh scenario0* scenario1* scenario2* scenario3* scenario4* scenario5* scenario6* >> $logFileName

endsec=$(date +%s)

echo 
echo "System tests were executed; results are above.  See the wres.log file for WRES log messages."
echo "Elapsed time: $(($endsec - $startsec)) seconds"
echo "Testing messages can be found here:"
echo 
echo "$logFileName"
echo

