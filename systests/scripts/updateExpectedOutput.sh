# This script copies all of the output directories from the scenario* directories and copies them to a 
# folder under /wres_share/testing that contains the revision name.  This will allow for defining 
# expected outputs relative to revision numbers so that images can be compared later to confirm that
# they are being created as expected.

revision=$(./wres.sh donothing | grep -m 1 version | cut -c 14-29)
if [[ $? != 0 ]]; then
    echo "Unable to execute wres.sh to acquire revision number.  Aborting..."
    exit 1
fi

targetDir=/wres_share/testing/systestsExpectedOutputs/systests_expected_outputs.$revision
echo "The directory of expected output to be created is $targetDir"
echo "Making..."
mkdir $targetDir
if [[ $? != 0 ]]; then
    echo "Unable to create expected output directory, $targetDir.  Aborting..."
    exit 1
fi

echo "Populating..."
for name in $(ls -d scenario*); do
    echo "  Processing $name..."
    mkdir $targetDir/$name
    cp -r $name/output $targetDir/$name
done
echo "Done creating and populating $targetDir."


