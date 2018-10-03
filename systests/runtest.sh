# This script is designed for execution on the WRES test VM within the Git cloned directory .../wres/systests.
# Arguments specify scenario subdirectories of systests and can be more than one.  It will look through the 
#   arguments provided in order.

# Read the options, which is currently only one: -l to indicate a run of latest. 
latest=0
while getopts "l" option; do
     case "${option}"
     in
         l) latest=1;
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
    executeDir=$systestsDir/$scenarioName

    # ============================================================
    # Preconditions
    # ============================================================
    if [ ! -d $executeDir ]; then
        echo "$echoPrefix Scenario provided, $scenarioName, does not exist in $(pwd).  Aborting..." | tee /dev/stderr
        continue 
    fi
    if [ ! -f $executeDir/$configName ]; then
        echo "$echoPrefix Scenario project config file, $executeDir/$configName, does not exist.  Aborting..." | tee /dev/stderr
        continue
    fi

    # ============================================================
    # Setup Scenario
    # ============================================================

    # Clear all old output directories if they exist.
    for directory in $executeDir/${outputDirPrefix}*
    do
        if [ -d $directory ]
        then
            echo "$echoPrefix Removing existing directory $directory"
            rm -rf $directory
        fi
    done

    # Remove older (pre-2018-10-02 output directory as well if needed)
    if [ -d $executeDir/output ]
    then
        echo "$echoPrefix Removing existing old output directory $executeDir/output"
        rm -rf $executeDir/output
    fi

    # Move into the directory.
    echo "$echoPrefix Moving (cd) to execution directory..."
    cd $executeDir

    # ============================================================
    # Process 0-length files, such as CLEAN
    # ============================================================

    # Do the clean if CLEAN is found in the scenario directory.
    if [ -z "$WRES_DB_NAME" ]
    then
    	export WRES_DB_NAME=wres4
    fi

    if [ -f CLEAN ]; then
        echo "$echoPrefix Cleaning the database: ../wres.sh cleandatabase ..."
        ../wres.sh cleandatabase
        if [[ $? -ne 0 ]]; then
            echo "$echoPrefix WRES clean failed; see above.  Something is wrong with the database $WRES_DB_NAME.  Aborting all tests..." | tee /dev/stderr
            exit 1
        fi
    fi

    # ============================================================
    # Run the "before" script if it exists and is executable.
    # ============================================================

    before_script="before.sh"
    if [[ -x $before_script ]]
    then
        echo "$echoPrefix Found a script to run before this test. Running it."
        ./$before_script
    fi

    # ============================================================
    # Execute the project.
    # ============================================================

    # If it fails then the file FAILS must exist in the scenario directory or its treated
    # as a test failure.  the file FAILS tells this script that failure is expected.
    echo "$echoPrefix Executing the project: ../wres.sh execute $configName ..."
    JAVA_OPTS='-Djava.io.tmpdir=.' ../wres.sh execute $configName
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

    after_script="after.sh"
    if [[ -x $after_script ]]
    then
        echo "$echoPrefix Found a script to run after this test. Running it."
        ./$after_script
    fi


    # ====================================================
    # Benchmark comparisons.
    # ====================================================
    cd ${outputDirPrefix}*

    #List the contents of the output directory and compare with contents in benchmark.
    echo "$echoPrefix Listing the output contents: ls *.csv *.png > dirListing.txt"
    ls *.csv *.png > dirListing.txt 2>/dev/null 

    #Compare files by calling the script.
    cd $executeDir
    ../scripts/determinePairsDiff.sh
    comparisonResult=$?

    if [ $comparisonResult -ne 0 ]
    then
        echo "$echoPrefix Test $scenarioName comparison FAILED with exit code $comparisonResult TEST FAILS!"
        overallResult=1
    fi

    endsec=$(date +%s)
    echo "$echoPrefix Test completed in $(($endsec - $startsec)) seconds" | tee /dev/stderr

done

if [ $overallResult -eq 0 ]
then
    echo "$echoPrefix All tests passed."
else
    echo "$echoPrefix One or more tests FAILED."
fi

exit $overallResult
