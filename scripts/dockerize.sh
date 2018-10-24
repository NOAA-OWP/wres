#!/bin/bash

# The purpose of this script is to avoid manual errors when tagging an image.
# It is not a replacement for understanding docker images, containers, and tags.
# Below steps can be run manually as well, and should be run when errors are not
# visible.

# Script needs to be run from the root of wres directory so that other scripts
# that depend on the context of the wres directory will work (such as those that
# depend on git depending on the context of directory to work).

# Depends on versions.sh script

# Attempt to auto-detect the versions needed.
all_versions=$( scripts/versions.sh )

overall_version=$( echo "$all_versions" | grep Main | cut -d' ' -f3 )
worker_shim_version=$( echo "$all_versions" | grep wres-worker | cut -d' ' -f3 )
tasker_version=$( echo "$all_versions" | grep wres-tasker | cut -d' ' -f3 )
broker_version=$( echo "$all_versions" | grep wres-broker | cut -d' ' -f3 )

# Sometimes auto-detection of versions does not work, because if no code changed
# then gradle will not create a new zip file. So the caller must specify each
# version with positional args, or "auto" to retain auto-detected version.

if [[ "$1" != "" && "$1" != "auto" ]]
then
    overall_version=$1
fi

if [[ "$2" != "" && "$2" != "auto" ]]
then
    worker_shim_version=$2
fi

if [[ "$3" != "" && "$3" != "auto" ]]
then
    tasker_version=$3
fi

if [[ "$4" != "" && "$4" != "auto" ]]
then
    broker_version=$4
fi


echo "overall_version is $overall_version"
echo "worker_shim_version is $worker_shim_version"
echo "tasker_version is $tasker_version"
echo "broker_version is $broker_version"

wres_core_file=wres-${overall_version}.zip
worker_shim_file=wres-worker-${worker_shim_version}.zip
tasker_file=wres-tasker-${tasker_version}.zip

jenkins_workspace=https://***REMOVED***/jenkins/job/Verify_OWP_WRES/ws
core_url=$jenkins_workspace/build/distributions/$wres_core_file
worker_url=$jenkins_workspace/wres-worker/build/distributions/$worker_shim_file
tasker_url=$jenkins_workspace/wres-tasker/build/distributions/$tasker_file

# Ensure the distribution zip files are present for successful docker build
while [[ ! -f build/distributions/$wres_core_file
         || ! -f wres-worker/build/distributions/$worker_shim_file \
         || ! -f wres-tasker/build/distributions/$tasker_file ]]
do
    # Allow for ctrl-c interrupt to take effect
    sleep 1
    read -n1 -r -p "Please download the files $core_url, $worker_url, and $tasker_url from the official build server and place them into their directories (build/distributions, wres-worker/build/distributions, and wres-tasker/build/distributions, respectively). After they have completely finished downloading and have been completely copied into the local directories, press any key to continue."
done

# Build and tag the worker image which is composed of WRES core and worker shim.
# Tag will be based on the later image version which is WRES core at git root.
echo "Building and tagging worker image..."
worker_image_id=$( docker build --build-arg version=$overall_version --build-arg worker_version=$worker_shim_version --quiet --tag wres/wres-worker:$overall_version . )
echo "Built wres/wres-worker:$overall_version -- $worker_image_id"

# Build and tag the tasker image which solely contains the tasker.
echo "Building tasker image..."
pushd wres-tasker
tasker_image_id=$( docker build --build-arg version=$tasker_version --quiet --tag wres/wres-tasker:$tasker_version . )
popd

echo "Built wres/wres-tasker:$tasker_version -- $tasker_image_id"

# Build and tag the broker image
echo "Building broker image..."
pushd wres-broker
broker_image_id=$( docker build --build-arg version=$broker_version --quiet --tag wres/wres-broker:$broker_version . )
popd

echo "Built wres/wres-broker:$broker_version -- $broker_image_id"

echo "Displaying most recent 10 docker images"
docker image ls | head -n 11
