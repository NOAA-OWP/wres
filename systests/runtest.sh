# This script is designed for execution on the WRES test VM within the Git cloned directory .../wres/systests.
# Arguments specify scenario subdirectories of systests and can be more than one.  It will look through the 
#   arguments provided in order.

# Options include the following:
#
# -l ... Use the latest installed revision, that pointed to by /wres_share/releases/latest
# -n ... No clean option turns off database cleaning
# -m ... No migration option turns off database migration, which is how WRES will be run in deployment.
#        Normally every run of the tests will have the potential to migrate the db through Liquibase.
latest=0
clean=1
migrate=1
while getopts ":lnm" option; do
     case "${option}"
     in
         l) latest=1;;
         n) clean=0;;
         m) migrate=0;;
     esac
done
shift $((OPTIND -1))

# Handle a latest run request by clearing out the working rls
if [[ "$latest" == 1 ]]; then
    echo "Removing the working release link, ~/wresTestScriptWorkingRelease, if it exists so that the latest release of WRES is used."
    rm -f ~/wresTestScriptWorkingRelease
fi

# Always cd to the directory containing this script to execute!
if [ ! -f runtest.sh ]; then
    echo "You are not in the directory that contains this run script.  Aborting..."
    exit 1;
fi

# ============================================================
# Arguments
# ============================================================
systestsDir=$(pwd)
configName=project_config.xml
outputDirPrefix=wres_evaluation_output_
benchDirName=benchmarks
echoPrefix="===================="

migrationArg="-Dwres.attemptToMigrate=true"
if [[ "$migrate" == 0 ]]; then
    echo "Liquibase migration of database will not be performed."
    migrationArg="-Dwres.attemptToMigrate=false"
else 
    echo "Liquibase migration of database will be performed since no -m was specified."
fi

# ============================================================
# Overall Setup
# ============================================================

overallResult=0

# Put the latest release in place next to the execution directory but only if it doesn't exist.
if [ ! -f $systestsDir/wres.sh ]; then
    echo "$echoPrefix Creating link $systestsDir/wres.sh, ln -s /wres_share/releases/wres.sh $systestsDir/wres.sh ..."
    echo "(If this is not executed on the WRES test VM, then this link will most likely not work.)"
    ln -s /wres_share/releases/wres.sh $systestsDir/wres.sh
fi

# Put a link to the testing data directory in place if it does not exist.
if [ ! -f $systestsDir/data ] && [ ! -d $systestsDir/data ]; then
    echo "$echoPrefix Creating link $systestsDir/data, ln -s /wres_share/testing/data $systestsDir/data ..." 
    echo "(If this is not executed on the WRES test VM, then this link will most likely not work.)"
    ln -s /wres_share/testing/data $systestsDir/data
fi


# ============================================================
# Loop over provided scenarios
# ============================================================
for scenarioName in $*; do

    echo    
    echo "##########################################################################"
    echo "PROCESSING $scenarioName..." | tee /dev/stderr
    echo "##########################################################################"
    echo

    startsec=$(date +%s) 
    scenarioDir=$systestsDir/$scenarioName

    # ============================================================
    # Preconditions
    # ============================================================
    if [ ! -d $scenarioDir ]; then
        echo "$echoPrefix Scenario provided, $scenarioName, does not exist in $systestsDir.  Aborting..." | tee /dev/stderr
        continue 
    fi
    if [ ! -f $scenarioDir/$configName ]; then
        echo "$echoPrefix Scenario project config file, $scenarioDir/$configName, does not exist.  Aborting..." | tee /dev/stderr
        continue
    fi

    # ============================================================
    # Setup Scenario
    # ============================================================

    # Clear all old output directories if they exist.
    for directory in $scenarioDir/${outputDirPrefix}*
    do
        if [ -d $directory ]
        then
            echo "$echoPrefix Removing existing directory $directory"
            rm -rf $directory
        fi
    done

    # Remove older (pre-2018-10-02 output directory as well if needed)
    if [ -d $scenarioDir/output ]
    then
        echo "$echoPrefix Removing existing old output directory $scenarioDir/output"
        rm -rf $scenarioDir/output
    fi

    # Move into the directory.
    echo "$echoPrefix Moving (cd) to execution directory..."
#    cd $systestsDir

    # ============================================================
    # Process 0-length files, such as CLEAN
    # ============================================================

    # Do the clean if CLEAN is found in the scenario directory.
    if [ -z "$WRES_DB_NAME" ]
    then
    	export WRES_DB_NAME=wres4 #Standard system testing db.
    fi

    if [[ "$clean" == 1 && -f $scenarioDir/CLEAN ]]; then
        echo "$echoPrefix Cleaning the database: ./wres.sh cleandatabase ..."
        JAVA_OPTS=$JAVA_OPTS" $migrationArg" ./wres.sh cleandatabase
        if [[ $? -ne 0 ]]; then
            echo "$echoPrefix WRES clean failed; see above.  Something is wrong with the database $WRES_DB_NAME.  Aborting all tests..." | tee /dev/stderr
            exit 1
        fi
    fi

    # ============================================================
    # Run the "before" script if it exists and is executable.
    # ============================================================

    before_script="$scenarioDir/before.sh"
    if [[ -x $before_script ]]
    then
        echo "$echoPrefix Found a script to run before this test. Running it from the scenario directory, $scenarioDir."
        cd $scenarioDir
        $before_script
        cd $systestsDir
    fi

    # ============================================================
    # Execute the project.
    # ============================================================

    # If it fails then the file FAILS must exist in the scenario directory or its treated
    # as a test failure.  the file FAILS tells this script that failure is expected.
    echo "$echoPrefix Executing the project: ./wres.sh execute $scenarioDir/$configName ..."
    JAVA_OPTS=$JAVA_OPTS" -Djava.io.tmpdir=$scenarioDir $migrationArg" ./wres.sh execute $scenarioDir/$configName
    executeResult=$?

    if [[ executeResult -ne 0 ]]; then
        if [ -f FAILS ]; then
            echo "$echoPrefix Expected failure occurred.  Test passes."
        else 
            echo "$echoPrefix WRES execution failed; see log file.  TEST FAILS!" | tee /dev/stderr
            # No attempt at this level to distinguish failures, use exit 1
            overallResult=1
        fi

        endsec=$(date +%s)
        echo "$echoPrefix Test completed in $(($endsec - $startsec)) seconds" | tee /dev/stderr
        continue 
    fi

    #If this if is triggered, then the execute was successful but the FAILS flag was set for this test.  Test fails.
    if [ -f FAILS ]; then
        echo "$echoPrefix Expected a WRES failure, but it did not fail! TEST FAILS!" | tee /dev/stderr
        endsec=$(date +%s)
        echo "$echoPrefix Test completed in $(($endsec - $startsec)) seconds" | tee /dev/stderr
        continue
    fi


    # ============================================================
    # Run the "after" script if it exists and is executable.
    # ============================================================

    after_script="$scenarioDir/after.sh"
    if [[ -x $after_script ]]
    then
        echo "$echoPrefix Found a script to run after this test. Running it from the scenario directory, $scenarioDir."
        cd $scenarioDir
        $after_script
        cd $systestsDir
    fi


    # ====================================================
    # Benchmark comparisons.
    # ====================================================
    cd $scenarioDir/${outputDirPrefix}*

    #List the contents of the output directory and compare with contents in benchmark.
    echo "$echoPrefix Listing the output contents: ls *.csv *.png > dirListing.txt"
    ls *.csv *.png > dirListing.txt 2>/dev/null 

    #Compare files by calling the script, which assumes we are in the scenario directory.
    cd $scenarioDir
    ../scripts/determinePairsDiff.sh
    comparisonResult=$?

    if [ $comparisonResult -ne 0 ]
    then
        echo "$echoPrefix Test $scenarioName comparison FAILED with exit code $comparisonResult. TEST FAILS!" | tee /dev/stderr
        overallResult=1
    fi

    endsec=$(date +%s)
    echo "$echoPrefix Test completed in $(($endsec - $startsec)) seconds" | tee /dev/stderr
    
    #Return to the system testing directory. 
    cd $systestsDir
done

if [ $overallResult -eq 0 ]
then
    echo "All tests passed."
else
    echo | tee /dev/stderr
    echo "One or more tests FAILED." | tee /dev/stderr
fi

# added by RHC, archive each run results
#WHOAMI=`/bin/whoami`
# Only when wres-cron running this wcript during cron job archive the test results.
# Other developers no need to archive the test results
#if [ "$WHOAMI" = "wres-cron" ]
#then
#	echo "$WHOAMI will archive the test results"
#	evaluationDir=`ls -d wres_evaluation_output*`
#	if [ -d $evaluationDir ]
#	then
#		WHEREAMI=`pwd | gawk -F/ '{print $NF}'`
#		#filename=`echo $evaluationDir | cut -d'_' -f1-4`
#		filename=`echo "$WHEREAMI"_"$evaluationDir"`
#		tar -czf "$filename".tar.gz $evaluationDir/
#		mkdir -pv ../SystemTestsOutputs
#		mv -v "$filename".tar.gz ../SystemTestsOutputs/ 
#	fi
#else
#	echo "You are $WHOAMI"
#fi

exit $overallResult
