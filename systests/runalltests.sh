# Executes all system tests scenario except for the 900 series, for which the tests are a bit long.
# Run this within the Git cloned directory .../wres/systests, where the runtest.sh script is found.

logFileName="wresSysTestResults_$(date +"%Y%m%d_%H%M%S").txt"

touch $logFileName
echo "System test results for scenarios:" > $logFileName
ls -d scenario0* scenario1* scenario2* scenario3* scenario4* scenario5* >> $logFileName

echo "System test results for scenarios:" 
ls -d scenario0* scenario1* scenario2* scenario3* scenario4* scenario5* scenario6*

startsec=$(date +%s)

./runtest.sh scenario0* scenario1* scenario2* scenario3* scenario4* scenario5* >> $logFileName

endsec=$(date +%s)

echo 
echo "System tests were executed; results are above.  See the wres.log file for WRES log messages."
echo "Elapsed time: $(($endsec - $startsec)) seconds"
echo "Testing messages can be found here:"
echo 
echo "$logFileName"
echo

