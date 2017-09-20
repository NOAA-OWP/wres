# This script is designed for execution on the WRES test VM in the directory ~/wres/systests. 

# Always cd to the directory containing this script to execute!
if [ ! -f runtest.sh ]; then
    echo "You are not in the directory that contains this run script.  Aborting..."
    exit 1;
fi


scenarioName=$1
systestsDir=.
executeDir=$systestsDir/$1
configName=project_config.xml
outputDirName=output
benchDirName=benchmarks
echoPrefix="===================="

#Check stuff
if [ ! -d $executeDir ]; then
    echo "$echoPrefix Scenario provided, $scenarioName, does not exist in $(pwd).  Aborting..."
    exit 1;
fi
if [ ! -f $executeDir/$configName ]; then
    echo "$echoPrefix Scenario project config file, $executeDir/$configName, does not exist.  Aborting..."
    exit 1;
fi

#Put the latest release in place next to hte execution directory but only if it doesn't exist.
if [ ! -f $executeDir/../wres.sh ]; then
    echo "$echoPrefix Creating link $systestsDir/wres.sh, ln -s /wres_share/releases/wres.sh $systestsDir/wres.sh ..."
    echo "(If this is not executed on the WRES test VM, then this link will most likely not work.)"
    ln -s /wres_share/releases/wres.sh $systestsDir/wres.sh
fi

#Put a link to the testing data directory in place if it does not exist.
if [ ! -f $executeDir/../data && ! -d $executeDir/../data ]; then
    echo "$echoPrefix Creating link $systestsDir/data, ln -s /wres_share/testing/data $systestsDir/data ..."
    echo "(If this is not executed on the WRES test VM, then this link will most likely not work.)"
    ln -s /wres_share/testing/data $systestsDir/data
fi

#Clearing the output directory.
echo "$echoPrefix Removing and recreating $executeDir/output of .csv and .png files only..."
if [ -d $executeDir/output ]; then
    rm -rf $executeDir/output
fi
mkdir $executeDir/output

#Move into the directory.
echo "$echoPrefix Moving (cd) to execution directory..."
cd $executeDir

#Do the clean if CLEAN is found in the scenario directory.
if [ -f CLEAN ]; then
    echo "$echoPrefix Cleaning the database: ../wres.sh cleandatabase ..."
    ../wres.sh cleandatabase
    if [[ $? != 0 ]]; then
        echo "$echoPrefix WRES clean failed; see above.  Aborting test..."
        exit 1
    fi
fi

#Execute the project.  If it fails then the file FAILS must exist in the scenario directory or its treated
#as a test failure.  the file FAILS tells this script that failure is expected.
echo "$echoPrefix Executing the project: ../wres.sh execute $configName ..."
../wres.sh execute $configName
if [[ $? != 0 ]]; then
    if [ -f FAILS ]; then
        echo "$echoPrefix Expected failure occured.  Test passes."
        exit 0
    fi
    echo "$echoPrefix WRES execution failed; see above.  Aborting..."
    exit 1
fi

#If this if is triggered, then the execute was successful but the FAILS flag was set for this test.  Test fails.
if [ -f FAILS ]; then
    echo "$echoPrefix Expected a failure, but it did not fail! TEST FAILS! Aborting..."
    exit 1
fi

#Benchmark comparisons.
numOutputFiles=$(ls $outputDirName | wc -l)
numBenchmarkFiles=$(ls $benchDirName | wc -l)
if [[ $numOutputFiles != $numBenchmarkFiles ]]; then
    echo "$echoPrefix Number of output files does not match: test = $numOutputFiles; benchmarks = $numBenchmarkFiles.  TEST FAILS!"
    exit 1
fi
echo "$echoPrefix Comparing output .csv files..."
cd $outputDirName
for fileName in $(ls *.csv); do
    diff --brief $fileName ../$benchDirName/$fileName
done
echo "$echoPrefix Comparing output .png files..."
for fileName in $(ls *.png); do
    diff --brief $fileName ../$benchDirName/$fileName
done


