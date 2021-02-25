#!/bin/bash

# dockerize.sh
# 
# Usage: 
#
#   cd <the top level of the WRES clone (i.e., where the scripts directory is located).>
#   scripts/dockerize.sh <core version (ver.)> <worker shim ver.> <tasker ver.> <broker ver.> <redis ver.> <events broker ver.> <graphics client ver.>
#
# Arguments: 
#  
# All arguments are optional, so that if one is not specified, then it is assumed 
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
# It is not a replacement for understanding docker images, containers, and tags.
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
#

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

wres_core_version=$overall_version
wres_tasker_version=$tasker_version

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
    graphics_version=$7
fi

echo "Core WRES binary zip version is $wres_core_version"
echo "WRES Worker shim binary zip version is $wres_worker_shim_version"
echo "WRES Tasker binary zip version is $wres_tasker_version"
echo "Primary docker image version is $overall_version"
echo "Tasker docker image version is $tasker_version"
echo "Broker docker image version is $broker_version"
echo "Redis docker image version is $redis_version"
echo "WRES events broker docker image version is $eventsbroker_version"
echo "WRES graphics client docker image version is $graphics_version"


#=============================================================
# Identify zip files and Jenkins URLs; wait for zips
#=============================================================

wres_core_file=wres-${wres_core_version}.zip
worker_shim_file=wres-worker-${wres_worker_shim_version}.zip
tasker_file=wres-tasker-${wres_tasker_version}.zip
graphics_file=wres-vis-${graphics_version}.zip

jenkins_workspace=https://***REMOVED***/jenkins/job/Verify_OWP_WRES/ws
core_url=$jenkins_workspace/build/distributions/$wres_core_file
worker_url=$jenkins_workspace/wres-worker/build/distributions/$worker_shim_file
tasker_url=$jenkins_workspace/wres-tasker/build/distributions/$tasker_file
graphics_url=$jenkins_workspace/wres-vis/build/distributions/$graphics_file

# Ensure the distribution zip files are present for successful docker build
while [[ ! -f ./build/distributions/$wres_core_file || \
         ! -f ./wres-worker/build/distributions/$worker_shim_file || \
         ! -f ./wres-tasker/build/distributions/$tasker_file || \
         ! -f ./wres-vis/build/distributions/$graphics_file  ]]
do
    # Allow for ctrl-c interrupt to take effect
    sleep 1
    echo ""
    echo "Please download these files and place them in the stated directory:"
    echo ""
    echo "    $core_url  -  build/distributions"
    echo "    $worker_url  -  wres-worker/build/distributions"
    echo "    $tasker_url  -  wres-tasker/build/distributions"
    echo "    $graphics_url  -  wres-vis/build/distributions"
    echo ""
    echo "You can use the following curl commands, with user name and token specified, to obtain the files:"
    echo ""
    echo "     curl -u <user.name>:<Jenkins API token> -o ./build/distributions/$wres_core_file $core_url"
    echo "     curl -u <user.name>:<Jenkins API token> -o ./wres-worker/build/distributions/$worker_shim_file $worker_url"
    echo "     curl -u <user.name>:<Jenkins API token> -o ./wres-tasker/build/distributions/$tasker_file $tasker_url"
    echo "     curl -u <user.name>:<Jenkins API token> -o ./wres-vis/build/distributions/$graphics_file $graphics_url"
    echo ""
    echo "You can also use the --config option instead of -u and put your user information in a file."
    echo ""
    echo "The above URLs are only valid if your .zip files are the latest artifact.  To pull down old artifacts, identify the Jenkins build number associated with the VLab GIT revision and modify the \"ws\" in the url to be \"<build number>/artifact\".  For example,"
    echo ""
    echo "https://***REMOVED***/jenkins/job/Verify_OWP_WRES/3686/artifact/wres-vis/build/distributions/wres-vis-20210225-713c981.zip"
    echo ""
    read -n1 -r -p "After they have completely finished downloading and have been completely copied into the local directories, press any key to continue.  Press ctrl-c to abort."
done


#=============================================================
# Build the images
#=============================================================

# Build and tag the worker image which is composed of WRES core and worker shim.
# Tag will be based on the later image version which is WRES core at git root.
echo "Building and tagging worker image..."
worker_image_id=$( docker build --build-arg version=$wres_core_version --build-arg worker_version=$wres_worker_shim_version --quiet --tag wres/wres-worker:$overall_version . )
echo "Built wres/wres-worker:$overall_version -- $worker_image_id"

# Build and tag the tasker image which solely contains the tasker.
echo "Building tasker image..."
pushd wres-tasker
tasker_image_id=$( docker build --build-arg version=$wres_tasker_version --quiet --tag wres/wres-tasker:$tasker_version . )
popd

echo "Built wres/wres-tasker:$tasker_version -- $tasker_image_id"

# Build and tag the broker image
echo "Building broker image..."
pushd wres-broker
broker_image_id=$( docker build --build-arg version=$broker_version --quiet --tag wres/wres-broker:$broker_version . )
popd

echo "Built wres/wres-broker:$broker_version -- $broker_image_id"

# Build and tag the redis image
echo "Building redis image..."
pushd wres-redis
redis_image_id=$( docker build --build-arg version=$redis_version --quiet --tag wres/wres-redis:$redis_version . )
popd

echo "Built wres/wres-redis:$redis_version -- $redis_image_id"

# Build and tag the eventsbroker image
echo "Building broker image..."
pushd wres-eventsbroker
eventsbroker_image_id=$( docker build --build-arg version=$eventsbroker_version --quiet --tag wres/wres-eventsbroker:$eventsbroker_version . )
popd

echo "Built wres/wres-eventsbroker:$eventsbroker_version -- $eventsbroker_image_id"

# Build and tag the graphics image
echo "Building graphics image..."
pushd wres-vis
graphics_image_id=$( docker build --build-arg version=$graphics_version --quiet --tag wres/wres-graphics:$graphics_version . )
popd

echo "Built wres/wres-graphics:$graphics_version -- $graphics_image_id"

echo "Displaying most recent 20 docker images"
docker image ls | head -n 21


#=============================================================
# Docker Registry
#=============================================================

# Optional: set environment variable DOCKER_REGISTRY to the FQDN of a docker
# registry (without any path, full fqdn, without scheme)

if [[ ! -z "$DOCKER_REGISTRY" ]]
then
    echo "Attempting more tags and docker push to https://$DOCKER_REGISTRY"
    echo "Now running docker login https://$DOCKER_REGISTRY..."
    docker login https://$DOCKER_REGISTRY
    login_success=$?

    if [[ ! login_success ]]
    then
        echo "Failed to login, not going to try to push to registry. Try again."
        exit 2
    fi

    primary_image_dev_status=$( echo ${overall_version} | grep "dev" )

    if [[ "$primary_image_dev_status" != "" ]]
    then
        echo "Refusing to tag and push primary docker image version ${overall_version} because its Dockerfile has not been committed to the repository yet."
    else
        echo "Tagging wres/wres-worker:$overall_version as $DOCKER_REGISTRY/wres/wres-worker/$overall_version"
        docker tag wres/wres-worker:$overall_version $DOCKER_REGISTRY/wres/wres-worker:$overall_version
	docker push $DOCKER_REGISTRY/wres/wres-worker:$overall_version
    fi

    tasker_image_dev_status=$( echo ${tasker_version} | grep "dev" )

    if [[ "$tasker_image_dev_status" != "" ]]
    then
        echo "Refusing to tag and push tasker docker image version ${tasker_version} because its Dockerfile has not been committed to the repository yet."
    else
        echo "Tagging wres/wres-tasker:$tasker_version as $DOCKER_REGISTRY/wres/wres-tasker/$tasker_version"
	docker tag wres/wres-tasker:$tasker_version $DOCKER_REGISTRY/wres/wres-tasker:$tasker_version
        docker push $DOCKER_REGISTRY/wres/wres-tasker:$tasker_version
    fi

    broker_image_dev_status=$( echo ${broker_version} | grep "dev" )

    if [[ "$broker_image_dev_status" != "" ]]
    then
        echo "Refusing to tag and push broker docker image version ${broker_version} because its Dockerfile has not been committed to the repository yet."
    else
        echo "Tagging wres/wres-broker:$broker_version as $DOCKER_REGISTRY/wres/wres-broker/$broker_version"
        docker tag wres/wres-broker:$broker_version $DOCKER_REGISTRY/wres/wres-broker:$broker_version
        docker push $DOCKER_REGISTRY/wres/wres-broker:$broker_version
    fi

    redis_image_dev_status=$( echo ${redis_version} | grep "dev" )

    if [[ "$redis_image_dev_status" != "" ]]
    then
        echo "Refusing to tag and push redis docker image version ${redis_version} because its Dockerfile has not been committed to the repository yet."
    else
        echo "Tagging wres/wres-redis:$redis_version as $DOCKER_REGISTRY/wres/wres-redis/$redis_version"
        docker tag wres/wres-redis:$redis_version $DOCKER_REGISTRY/wres/wres-redis:$redis_version
        docker push $DOCKER_REGISTRY/wres/wres-redis:$redis_version
    fi

    eventsbroker_image_dev_status=$( echo ${eventsbroker_version} | grep "dev" )

    if [[ "$eventsbroker_image_dev_status" != "" ]]
    then
        echo "Refusing to tag and push broker docker image version ${eventsbroker_version} because its Dockerfile has not been committed to the repository yet."
    else
        echo "Tagging wres/wres-eventsbroker:$eventsbroker_version as $DOCKER_REGISTRY/wres/wres-eventsbroker/$eventsbroker_version"
        docker tag wres/wres-eventsbroker:$eventsbroker_version $DOCKER_REGISTRY/wres/wres-eventsbroker:$eventsbroker_version
        docker push $DOCKER_REGISTRY/wres/wres-eventsbroker:$eventsbroker_version
    fi

    graphics_image_dev_status=$( echo ${graphics_version} | grep "dev" )

    if [[ "$graphics_image_dev_status" != "" ]]
    then
        echo "Refusing to tag and push broker docker image version ${graphics_version} because its Dockerfile has not been committed to the repository yet."
    else
        echo "Tagging wres/wres-graphics:$graphics_version as $DOCKER_REGISTRY/wres/wres-graphics/$graphics_version"
        docker tag wres/wres-graphics:$graphics_version $DOCKER_REGISTRY/wres/wres-graphics:$graphics_version
        docker push $DOCKER_REGISTRY/wres/wres-graphics:$graphics_version
    fi
    
    
else
    echo "No variable 'DOCKER_REGISTRY' found, not attempting to docker push."
    echo "If you want to automatically push, set DOCKER_REGISTRY to the FQDN of"
    echo "an accessible docker registry and this script will attempt to tag and"
    echo "push to that registry."
fi

#=============================================================
# Create .yml files
#=============================================================

read -n1 -r -p "Updating the two yml files, docker-compose-all-roles-with-graphics.yml and docker-compose-workers-only-with-graphics.yml.  These files are kept in the repository and will be created based on template files with image tags replaced.  Continue?  Press ctrl-c to abort this step."

cp docker-compose-all-roles-with-graphics.template.yml docker-compose-all-roles-with-graphics.yml 
sed -i "s/TASKER_IMAGE/${tasker_version}/" docker-compose-all-roles-with-graphics.yml
sed -i "s/BROKER_IMAGE/${broker_version}/" ddocker-compose-all-roles-with-graphics.yml
sed -i "s/REDIS_IMAGE/${redis_version}/" docker-compose-all-roles-with-graphics.yml
sed -i "s/WORKER_IMAGE/${wres_worker_shim_version}/" docker-compose-all-roles-with-graphics.yml
sed -i "s/EVENTS_IMAGE/${eventsbroker_version}/" docker-compose-all-roles-with-graphics.yml
sed -i "s/GRAPHICS_IMAGE/${graphics_version}/" docker-compose-all-roles-with-graphics.yml
sed -i "s/OVERALL_IMAGE/${overall_version}/" docker-compose-all-roles-with-graphics.yml

cp docker-compose-workers-only-with-graphics.template.yml docker-compose-workers-only-with-graphics.yml
sed -i "s/TASKER_IMAGE/${tasker_version}/" docker-compose-workers-only-with-graphics.yml
sed -i "s/BROKER_IMAGE/${broker_version}/" docker-compose-workers-only-with-graphics.yml
sed -i "s/REDIS_IMAGE/${redis_version}/" docker-compose-workers-only-with-graphics.yml
sed -i "s/WORKER_IMAGE/${wres_worker_shim_version}/" docker-compose-workers-only-with-graphics.yml
sed -i "s/EVENTS_IMAGE/${eventsbroker_version}/" docker-compose-workers-only-with-graphics.yml
sed -i "s/GRAPHICS_IMAGE/${graphics_version}/" docker-compose-workers-only-with-graphics.yml
sed -i "s/OVERALL_IMAGE/${overall_version}/" docker-compose-workers-only-with-graphics.yml

echo ""
echo "The two .yml files have been updated.  Please push them to the repository if appropriate."



