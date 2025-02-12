#!/bin/bash

# poderize.sh
# 
# Usage: 
#
#   cd <the top level of the WRES clone (i.e., where the scripts directory is located).>
#   scripts/poderize.sh <Jenkins build number> <core version (ver.)> <worker shim ver.> <tasker ver.> <broker ver.> <redis ver.> <events broker ver.> <graphics client ver.>
#
# Arguments:
#
# All other arguments are optional, so that if one is not specified, then it is assumed 
# to be "auto". A version of "auto" will result in the default version, obtained
# through the versions.sh script, being used.
#
# All arguments are positional.  For example, if you want to specify the tasker 
# version, then you need to specify the core and worker shim versions as "auto", 
# first.
#
# Description:
# 
# The purpose of this script is to avoid manual errors when tagging an image.
# It is not a replacement for understanding images, containers, and tags.
# Below steps can be run manually as well, and should be run when errors are not
# visible.
#
# Script needs to be run from the root of wres directory so that other scripts
# that depend on the context of the wres directory will work (such as those that
# depend on git depending on the context of directory to work).
#
# This script should be idempotent, meaning you can run it several times in
# a row without damage. This is true for the build steps, tag steps, push steps.
#
# Depends on versions.sh script


#=============================================================
# Identify default versions!
#=============================================================

# Attempt to auto-detect the versions needed.
all_versions=$( scripts/versions.sh )

overall_version=$( echo "$all_versions" | grep "^Main version" | cut -d' ' -f3 )
tasker_version=$( echo "$all_versions" | grep "^wres-tasker version" | cut -d' ' -f3 )
broker_version=$( echo "$all_versions" | grep "^wres-broker version" | cut -d' ' -f3 )
redis_version=$( echo "$all_versions" | grep "^wres-redis version" | cut -d' ' -f3 )
wres_worker_shim_version=$( echo "$all_versions" | grep "^wres-worker version" | cut -d' ' -f3 )
eventsbroker_version=$( echo "$all_versions" | grep "^wres-eventsbroker version" | cut -d' ' -f3 )
graphics_version=$( echo "$all_versions" | grep "^wres-vis version" | cut -d' ' -f3 )
writing_version=$( echo "$all_versions" | grep "^wres-writing version" | cut -d' ' -f3 )

# These will be the zip ids, as distinct from the previously-found image ids.
wres_core_version=$overall_version
wres_tasker_version=$tasker_version
wres_vis_version=$graphics_version
wres_writing_version=$writing_version


# Sometimes auto-detection of versions does not work, because if no code changed
# then gradle will not create a new zip file. So the caller must specify each
# version with positional args, or "auto" to retain auto-detected version.

if [[ "$1" != "" && "$1" != "auto" ]]
then
    wres_core_version=$1
fi

if [[ "$2" != "" && "$2" != "auto" ]]
then
    wres_worker_shim_version=$2
fi

if [[ "$3" != "" && "$3" != "auto" ]]
then
    wres_tasker_version=$3
fi

if [[ "$4" != "" && "$4" != "auto" ]]
then
    broker_version=$4
fi

if [[ "$5" != "" && "$5" != "auto" ]]
then
    redis_version=$5
fi

if [[ "$6" != "" && "$6" != "auto" ]]
then
    eventsbroker_version=$6
fi

if [[ "$7" != "" && "$7" != "auto" ]]
then
    wres_vis_version=$7
fi

if [[ "$8" != "" && "$8" != "auto" ]]
then
    wres_writing_version=$8
fi

echo ""
echo "VERSIONS USED BASED ON DEFAULTS WITH ARGUMENT OVERRIDES:"
echo ""
echo "Core WRES binary zip version is $wres_core_version"
echo "WRES Worker shim binary zip version is $wres_worker_shim_version"
echo "WRES Tasker binary zip version is $wres_tasker_version"
echo "Primary podman image version is $overall_version"
echo "Tasker podman image version is $tasker_version"
echo "Broker podman image version is $broker_version"
echo "Redis podman image version is $redis_version"
echo "WRES events broker podman image version is $eventsbroker_version"
echo "WRES vis binary zip version is $wres_vis_version"
echo "WRES graphics podman image version is $graphics_version"
echo "WRES writing podman image version is $wres_writing_version"


#=============================================================
# Identify zip files and Jenkins URLs; wait for zips
#=============================================================
echo ""
echo "Identifying .zip files required..."

wres_core_file=wres-${wres_core_version}.zip
worker_shim_file=wres-worker-${wres_worker_shim_version}.zip
tasker_file=wres-tasker-${wres_tasker_version}.zip
vis_file=wres-vis-${wres_vis_version}.zip
writing_file=wres-writing-${wres_writing_version}.zip

# Ensure the distribution zip files are present for successful podman build --format docker
if [[ ! -f ./build/distributions/$wres_core_file || \
         ! -f ./wres-worker/build/distributions/$worker_shim_file || \
         ! -f ./wres-tasker/build/distributions/$tasker_file || \
         ! -f ./wres-writing/build/distributions/$writing_file || \
         ! -f ./wres-vis/build/distributions/$vis_file  ]]
then
    echo ""
    echo "It appears you are not an automated build server (or something went wrong if you are)."
    echo ""
    echo "You do not have one of the required files, check the bellow exist"
    echo ""
    echo "./build/distributions/$wres_core_file"
    echo "./wres-worker/build/distributions/$worker_shim_file"
    echo "./wres-tasker/build/distributions/$tasker_file"
    echo "./wres-writing/build/distributions/$writing_file"
    echo "./wres-vis/build/distributions/$vis_file"
    echo ""
    echo ""
    exit 3
fi


#=============================================================
# Build the images
#=============================================================
echo ""
echo "Building images..."

# Build and tag the worker image which is composed of WRES core and worker shim.
# Tag will be based on the later image version which is WRES core at git root.
echo "Building and tagging worker image..."
worker_image_id=$( podman build --format docker -f Poderfile --build-arg version=$wres_core_version --build-arg worker_version=$wres_worker_shim_version --quiet --tag wres/wres-worker:$overall_version . )
echo "Built wres/wres-worker:$overall_version -- $worker_image_id"

# Build and tag the tasker image which solely contains the tasker.
echo "Building tasker image..."
pushd wres-tasker
tasker_image_id=$( podman build --format docker -f Poderfile --build-arg version=$wres_tasker_version --tag wres/wres-tasker:$tasker_version . )
popd

echo "Built wres/wres-tasker:$tasker_version -- $tasker_image_id"

# Build and tag the broker image
echo "Building broker image..."
pushd wres-broker
broker_image_id=$( podman build --format docker -f Poderfile --pull --no-cache  --build-arg version=$broker_version --tag wres/wres-broker:$broker_version . )
popd

echo "Built wres/wres-broker:$broker_version -- $broker_image_id"

# Build and tag the redis image
echo "Building redis image..."
pushd wres-redis
redis_image_id=$( podman build --format docker -f Poderfile --pull --no-cache --build-arg version=$redis_version --tag wres/wres-redis:$redis_version . )
popd

echo "Built wres/wres-redis:$redis_version -- $redis_image_id"

# Build and tag the eventsbroker image
echo "Building events broker image..."
pushd wres-eventsbroker
eventsbroker_image_id=$( podman build --format docker -f Poderfile --no-cache --build-arg version=$eventsbroker_version --tag wres/wres-eventsbroker:$eventsbroker_version . )
popd

echo "Built wres/wres-eventsbroker:$eventsbroker_version -- $eventsbroker_image_id"

# Build and tag the graphics image
echo "Building graphics image..."
pushd wres-vis
graphics_image_id=$( podman build --format docker -f Poderfile --build-arg version=$wres_vis_version --tag wres/wres-graphics:$graphics_version . )
popd

echo "Built wres/wres-graphics:$graphics_version -- $graphics_image_id"

# Build and tag the writing image
echo "Building writing image..."
pushd wres-writing
writing_image_id=$( podman build --format docker -f Poderfile --build-arg version=$wres_writing_version --tag wres/wres-writing:$writing_version . )
popd

echo "Built wres/wres-writing:$writing_version -- $writing_image_id"

echo "Displaying most recent 20 docker images"
docker image ls | head -n 21


#=============================================================
# Docker Registry
#=============================================================

# Optional: set environment variable DOCKER_REGISTRY to the FQDN of a docker
# registry (without any path, full fqdn, without scheme)

if [[ ! -z "$DOCKER_REGISTRY" ]]
then
    # Check the format of the registry env var.  If something is wrong, then don't use it.
    if [[ $DOCKER_REGISTRY =~ ^https?:// ]]
#    if [[ $DOCKER_REGISTRY == http* ]]
    then
        echo ""
        echo "You provided a DOCKER_REGISTRY, but it starts with http. Don't include the scheme!"
        echo "Skipping pushing the images to the registry!"
    # It looks good, try to push to the registry.
    else
        echo ""
        echo "Attempting tagging and pushing images to the registry, https://$DOCKER_REGISTRY ..."
        echo "Running docker login https://$DOCKER_REGISTRY..."
        podman login https://$DOCKER_REGISTRY
        login_success=$?

        if [[ ! login_success ]]
        then
            echo "Failed to login, not going to try to push to registry. Try again."
            exit 2
        fi


        echo "Tagging and pushing wres/wres-worker:$overall_version as $DOCKER_REGISTRY/wres/wres-worker/$overall_version..."
        podman tag wres/wres-worker:$overall_version $DOCKER_REGISTRY/wres/wres-worker:$overall_version
        podman push $DOCKER_REGISTRY/wres/wres-worker:$overall_version


        echo "Tagging and pushing  wres/wres-tasker:$tasker_version as $DOCKER_REGISTRY/wres/wres-tasker/$tasker_version..."
        podman tag wres/wres-tasker:$tasker_version $DOCKER_REGISTRY/wres/wres-tasker:$tasker_version
        podman push $DOCKER_REGISTRY/wres/wres-tasker:$tasker_version


        echo "Tagging and pushing wres/wres-broker:$broker_version as $DOCKER_REGISTRY/wres/wres-broker/$broker_version..."
        podman tag wres/wres-broker:$broker_version $DOCKER_REGISTRY/wres/wres-broker:$broker_version
        podman push $DOCKER_REGISTRY/wres/wres-broker:$broker_version


        echo "Tagging and pushing wres/wres-redis:$redis_version as $DOCKER_REGISTRY/wres/wres-redis/$redis_version..."
        podman tag wres/wres-redis:$redis_version $DOCKER_REGISTRY/wres/wres-redis:$redis_version
        podman push $DOCKER_REGISTRY/wres/wres-redis:$redis_version


        echo "Tagging and pushing wres/wres-eventsbroker:$eventsbroker_version as $DOCKER_REGISTRY/wres/wres-eventsbroker/$eventsbroker_version..."
        podman tag wres/wres-eventsbroker:$eventsbroker_version $DOCKER_REGISTRY/wres/wres-eventsbroker:$eventsbroker_version
        podman push $DOCKER_REGISTRY/wres/wres-eventsbroker:$eventsbroker_version


        echo "Tagging and pushing wres/wres-graphics:$graphics_version as $DOCKER_REGISTRY/wres/wres-graphics/$graphics_version..."
        podman tag wres/wres-graphics:$graphics_version $DOCKER_REGISTRY/wres/wres-graphics:$graphics_version
        podman push $DOCKER_REGISTRY/wres/wres-graphics:$graphics_version


        echo "Tagging and pushing wres/wres-writing:$writing_version as $DOCKER_REGISTRY/wres/wres-writing/$writing_version..."
        podman tag wres/wres-writing:$writing_version $DOCKER_REGISTRY/wres/wres-writing:$writing_version
        podman push $DOCKER_REGISTRY/wres/wres-writing:$writing_version


        echo "Tagging and pushing wres/nginx as wres/nginx..."
        podman tag wres/nginx $DOCKER_REGISTRY/wres/nginx
        podman push $DOCKER_REGISTRY/wres/nginx
    fi
    
else
    echo ""
    echo "No variable 'DOCKER_REGISTRY' found, not attempting to docker push."
    echo "If you want to automatically push, set DOCKER_REGISTRY to the FQDN of"
    echo "an accessible docker registry and this script will attempt to tag and"
    echo "push to that registry."
    echo ""
fi

#=============================================================
# Create .yml files
#=============================================================

echo ""
echo "About to update the .yml files with the new versions based on a template."
echo "If you are only updating some of the images/versions, it is recommended"
echo "you skip this step and do that by manually editing the .ymls."
echo ""

cp compose-entry.template.yml compose-entry.yml 
sed -i "s/TASKER_IMAGE/${tasker_version}/" compose-entry.yml
sed -i "s/BROKER_IMAGE/${broker_version}/" compose-entry.yml
sed -i "s/REDIS_IMAGE/${redis_version}/" compose-entry.yml
sed -i "s/WORKER_IMAGE/${overall_version}/" compose-entry.yml # By design... The tag for the worker image is the "overall_version".
sed -i "s/EVENTS_IMAGE/${eventsbroker_version}/" compose-entry.yml
sed -i "s/GRAPHICS_IMAGE/${graphics_version}/" compose-entry.yml
sed -i "s/WRITING_IMAGE/${writing_version}/" compose-entry.yml
sed -i "s/OVERALL_IMAGE/${overall_version}/" compose-entry.yml

cp compose-workers.template.yml compose-workers.yml
sed -i "s/TASKER_IMAGE/${tasker_version}/" compose-workers.yml
sed -i "s/BROKER_IMAGE/${broker_version}/" compose-workers.yml
sed -i "s/REDIS_IMAGE/${redis_version}/" compose-workers.yml
sed -i "s/WORKER_IMAGE/${overall_version}/" compose-workers.yml # By design... The tag for the worker image is the "overall_version".
sed -i "s/EVENTS_IMAGE/${eventsbroker_version}/" compose-workers.yml
sed -i "s/GRAPHICS_IMAGE/${graphics_version}/" compose-workers.yml
sed -i "s/WRITING_IMAGE/${writing_version}/" compose-workers.yml
sed -i "s/OVERALL_IMAGE/${overall_version}/" compose-workers.yml

echo ""
echo "The two .yml files have been updated.  Please push them to the repository, if appropriate, or use 'git checkout' to undo the changes."



