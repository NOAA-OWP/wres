# This script is designed for execution on the WRES test VM within the Git cloned directory .../wres/systests.
# Arguments specify scenario subdirectories of systests and can be more than one.  It will look through the 
#   arguments provided in order.

# Read the options, which is currently only one: -l to indicate a run of latest. 
if [ $# -lt 1 ]
then
	echo "Usage: $0 [-l 0|1] [-d WRES_DB_NAME] [-t systestsDir] [-r revision] scenarios"
	exit 2
fi
latest=0
while getopts "l:d:t:r:" option; do
     case "${option}"
     in
         l)
		#latest=1
		latest=$OPTARG
		;;
	d)	
		export WRES_DB_NAME=$OPTARG
		;;
	t)
		systestsDir=$OPTARG
		;;	
	r)
		latest_noZip=$OPTARG
		;;
	?)
		echo "Unknown option -$OPTARG" >&2
		echo "Usage: $0 [-l 0|1] [-d WRES_DB_NAME] [-t systestsDir] [-r revision] scenarios"
		exit 2
		;;
     esac
done
shift $((OPTIND -1))

echo "latest = $latest, WRES_DB_NAME = $WRES_DB_NAME"
echo "systestDir = $systestsDir"
if [ -z $systestsDir ]
then
        systestsDir=$(pwd)
fi
cd $systestsDir
scenarios=`ls -d $*`

# Handle a latest run request by clearing out the working rls
if [[ "$latest" == 1 ]]; then
    echo "Removing the working release link, ~/wresTestScriptWorkingRelease, if it exists so that the latest release of WRES is used."
    rm -f ~/wresTestScriptWorkingRelease
fi

# Always cd to the directory containing this script to execute!
if [ ! -f runtest_db.sh ]; then
    echo "You are not in the directory that contains this run script.  Aborting..."
    exit 1;
fi

# ============================================================
# Arguments
# ============================================================
#if [ -z $systestsDir ]
#then
#	systestsDir=$(pwd)
#fi
configName=project_config.xml
outputDirName=output
benchDirName=benchmarks
echoPrefix="===================="

# ============================================================
# Overall Setup
# ============================================================

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

echo "latest_noZip = $latest_noZip"
echo "scenarios = $scenarios"

# ============================================================
# Loop over provided scenarios
# ============================================================
#for scenarioName in $*; do
for scenarioName in $scenarios; do

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
    # Clearing the output directory.
    echo "$echoPrefix Removing and recreating $executeDir/output of .csv and .png files only..."
    if [ -d $executeDir/output ]; then
        rm -rf $executeDir/output
    fi
    mkdir $executeDir/output

    # Move into the directory.
    echo "$echoPrefix Moving (cd) to execution directory..."
    cd $executeDir

    if [ -f running ]
    then # go to next if this one is running
	continue
    else
	touch running
    fi
    # ============================================================
    # Process 0-length files, such as CLEAN
    # ============================================================

    # Do the clean if CLEAN is found in the scenario directory.
    if [ -z "$WRES_DB_NAME" ]
    then
    	export WRES_DB_NAME=wrestest2
    fi
    if [ -f CLEAN ]; then
        echo "$echoPrefix Cleaning the database: ../wres_override.sh cleandatabase ..."
        ../wres_override.sh cleandatabase
        if [[ $? != 0 ]]; then
            echo "$echoPrefix WRES clean failed at $?; see above.  Something is wrong with the database $WRES_DB_NAME.  Aborting all tests..." | tee /dev/stderr
	    #tail -100 /home/wres-cron/wres_logs/wres_900.log
	    if [ -f running ]
	    then
	    	rm -v running
	    fi
            exit 1
	else
		echo "Finished clean database $WRES_DB_NAME"
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
    pwd
    echo "$echoPrefix Executing the project: ../wres_override.sh execute $configName ..."
    ../wres_override.sh execute $configName
    if [[ $? != 0 ]]; then
        if [ -f FAILS ]; then
            echo "$echoPrefix Expected failure occurred.  Test passes."
        else 
            echo "$echoPrefix WRES execution failed; see log file.  TEST FAILS!" | tee /dev/stderr
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
    cd $outputDirName

    #List the contents of the output directory and compare with contents in benchmark.
    echo "$echoPrefix Listing the output contents: ls *.csv *.png > dirListing.txt"
    ls *.csv *.png > dirListing.txt 2>/dev/null 
#    if [ -f ../$benchDirName/dirListing.txt ]; then
#        echo "$echoPrefix Comparing listing with benchmark expected contents: diff dirListing.txt  ../$benchDirName/dirListing.txt"  
#        diff --brief dirListing.txt ../$benchDirName/dirListing.txt | tee /dev/stderr
#    else 
#        echo "$echoPrefix No benchmark directory listing to compare against.  Cannot compare expected output listing." | tee /dev/stderr
#    fi

    #Compare all csv files except for *pairs* files.
#    echo "$echoPrefix Comparing output .csv files..."
    #for fileName in $(ls *.csv | grep -v "pairs"); do
#    for fileName in $(grep .csv dirListing.txt | grep -v "pairs"); do
#	if [ -f $fileName -a -f ../$benchDirName/$fileNmae ] # if file name also in benchmarks/
#	then
#        	diff --brief $fileName ../$benchDirName/$fileName | tee /dev/stderr
#	fi
#    done

    #Compare files by calling the script.
    cd $executeDir
    ../scripts/determinePairsDiff.sh

    endsec=$(date +%s)
    echo "$echoPrefix Test completed in $(($endsec - $startsec)) seconds" | tee /dev/stderr
    pwd
    if [ -f running ]
    then
	rm -v running
    fi
done

egrep -C 3 '(diff|FAIL|Aborting)' systests_900screenCatch_"$latest_noZip".txt > systests_900Results_"$latest_noZip".txt
if [ -s systests_900Results_"$latest_noZip".txt ]
then
	/usr/bin/zip systests_900screenCatch_"$latest_noZip".txt
	/usr/bin/mailx -S smtp=140.90.91.135 -s "systests 900 results from $latest_noZip" -a systests_900screenCatch_"$latest_noZip".txt.zip Raymond.Chui@***REMOVED***,Hank.Herr@***REMOVED***,james.d.brown@***REMOVED***,jesse.bickel@***REMOVED***,christopher.tubbs@***REMOVED***,Alexander.Maestre@***REMOVED***,sanian.gaffar@***REMOVED*** < systests_900Results_"$latest_noZip".txt 
else
	rm -v systests_900Results_"$latest_noZip".txt
fi
