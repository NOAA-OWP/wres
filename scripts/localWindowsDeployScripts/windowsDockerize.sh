#!/bin/bash

#=============================================================
# This dir has some useful items in order to do a local deploy of the wres-server on a windows machine
# Each of the directories in this main directory have the addition to the docker files that will translate
# the line endings of the files to be correct
#
# BEFORE USE:
# - You need to update the docker files following the examples provided in each of the main wres directories
# - Set up the directory structure laid out in the compose-entry/worker yml files
# - Set up all certs
# - Create a .env (See example) file for all of the vars described in the yml file generated and all certs required
# - run the following commands to correct the line endings of the two scripts used to create docker containers:
# sed -i 's/\r$//' scripts/localWindowsDeployScripts/windowsDockerize.sh
# sed -i 's/\r$//' scripts/localWindowsDeployScripts/windowsVersions.sh
# - This script will generate yml files (With the wrong data paths)
#   Look at compose-entry-windows-test.yml in this dir to see changes I needed to make
#
# USAGE:
# - From the main level of the dir run ./scripts/localWindowsDeployScripts/windowsDockerize.sh
# - Update your compose yml files to point to the new containers made by the script (If there are any, only needed after commits/pulls)
# -- Or create your own templates and update the file names at the bottom of this file
# - run the compose up docker command. Similar to one like this:
# docker-compose -f compose-entry-windows-test.yml up --scale worker=1 --scale eventsbroker=1 --scale graphics=1
#
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# THIS IS FOR ASSISTANCE ONLY, DO NOT PUSH THE FILES CREATED BY THIS OR THE UPDATED DOCKER FILES
#
# BEFORE PUSH:
# Stage all changes you have made, these is roughly the sequence I was following
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#
# git restore --staged *
# git add ALL_FILES_I_WANT_COMMITED
# git commit
#
# Now stash all temp files created
# git add *
# git stash
#
# Push your changes
#=============================================================

# Attempt to auto-detect the versions needed.
all_versions=$( scripts/localWindowsDeployScripts/windowsVersions.sh )
echo "$all_versions"
overall_version=$( echo "$all_versions" | grep "^Main version" | cut -d' ' -f3 )
tasker_version=$( echo "$all_versions" | grep "^wres-tasker version" | cut -d' ' -f3 )
broker_version=$( echo "$all_versions" | grep "^wres-broker version" | cut -d' ' -f3 )
redis_version=$( echo "$all_versions" | grep "^wres-redis version" | cut -d' ' -f3 )

# Worker container seems to only pick up the dev tag when there are changes to the core
# Added this to pick up my changes, but may need to be removed in some cases
wres_worker_shim_version=$( echo "$all_versions" | grep "^wres-worker version" | cut -d' ' -f3 )"-dev"

eventsbroker_version=$( echo "$all_versions" | grep "^wres-eventsbroker version" | cut -d' ' -f3 )
graphics_version=$( echo "$all_versions" | grep "^wres-vis version" | cut -d' ' -f3 )
wres_writing_version=$( echo "$all_versions" | grep "^wres-writing version" | cut -d' ' -f3 )
# These will be the zip ids, as distinct from the previously-found image ids.
wres_core_version=$overall_version
wres_tasker_version=$tasker_version
wres_vis_version=$graphics_version


#=============================================================
# Build the images
#=============================================================
echo ""
echo "Building images..."

# Build and tag the worker image which is composed of WRES core and worker shim.
# Tag will be based on the later image version which is WRES core at git root.
echo "Building and tagging worker image..."
worker_image_id=$( docker build -f Poderfile --build-arg version=$wres_core_version --build-arg worker_version=$wres_worker_shim_version --quiet --tag wres/wres-worker . )
echo "Built wres/wres-worker:$overall_version -- $worker_image_id"

# Build and tag the tasker image which solely contains the tasker.
echo "Building tasker image..."
pushd wres-tasker
tasker_image_id=$( docker build -f Poderfile --build-arg version=$wres_tasker_version --quiet --tag wres/wres-tasker . )
popd

echo "Built wres/wres-tasker:$tasker_version -- $tasker_image_id"

# Build and tag the broker image
echo "Building broker image..."
pushd wres-broker
broker_image_id=$( docker build -f Poderfile --pull --no-cache  --build-arg version=$broker_version --quiet --tag wres/wres-broker . )
popd

echo "Built wres/wres-broker:$broker_version -- $broker_image_id"

# Build and tag the redis image
echo "Building redis image..."
pushd wres-redis
redis_image_id=$( docker build -f Poderfile --pull --no-cache --build-arg version=$redis_version --quiet --tag wres/wres-redis . )
popd

echo "Built wres/wres-redis:$redis_version -- $redis_image_id"

# Build and tag the eventsbroker image
echo "Building events broker image..."
pushd wres-eventsbroker
eventsbroker_image_id=$( docker build -f Poderfile --no-cache --build-arg version=$eventsbroker_version --quiet --tag wres/wres-eventsbroker . )
popd

echo "Built wres/wres-eventsbroker:$eventsbroker_version -- $eventsbroker_image_id"

# Build and tag the graphics image
echo "Building graphics image..."
pushd wres-vis
graphics_image_id=$( docker build -f Poderfile --build-arg version=$wres_vis_version --quiet --tag wres/wres-graphics . )
popd

echo "Built wres/wres-graphics:$graphics_version -- $graphics_image_id"

# Build and tag the writing image
echo "Building writing image..."
pushd wres-writing
writing_image_id=$( docker build -f Poderfile --build-arg version=$wres_writing_version --quiet --tag wres/wres-writing . )
popd
echo "Built wres/wres-writing:$writing_version -- $writing_image_id"

# Build and tag the nginx image
echo "Building nginx image..."
pushd nginx
nginx_image_id=$( docker build --build-arg --quiet --tag wres/nginx . )
popd

echo "Built wres/nginx"

echo "Displaying most recent 20 docker images"
docker image ls | head -n 21


#=============================================================
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# IF YOU CREATE YOUR OWN TEMPLATES THEN YOU CAN UPDATE THIS SECTION
# TO HAVE THIS SCRIPT AUTOMATICALLY CREATE NEW COMPOSE FILES
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#=============================================================

cp podman-compose-entry.template.yml compose-entry-windows.yml
sed -i "s/TASKER_IMAGE/${tasker_version}/" compose-entry-windows.yml
sed -i "s/BROKER_IMAGE/${broker_version}/" compose-entry-windows.yml
sed -i "s/REDIS_IMAGE/${redis_version}/" compose-entry-windows.yml
sed -i "s/WORKER_IMAGE/${overall_version}/" compose-entry-windows.yml  # By design... The tag for the worker image is the "overall_version".
sed -i "s/EVENTS_IMAGE/${eventsbroker_version}/" compose-entry-windows.yml
sed -i "s/GRAPHICS_IMAGE/${graphics_version}/" compose-entry-windows.yml
sed -i "s/WRITING_IMAGE/${writing_version}/" compose-entry-windows.yml
sed -i "s/OVERALL_IMAGE/${overall_version}/" compose-entry-windows.yml

cp podman-compose-workers.template.yml compose-workers-windows.yml
sed -i "s/TASKER_IMAGE/${tasker_version}/" compose-workers-windows.yml
sed -i "s/BROKER_IMAGE/${broker_version}/" compose-workers-windows.yml
sed -i "s/REDIS_IMAGE/${redis_version}/" compose-workers-windows.yml
sed -i "s/WORKER_IMAGE/${overall_version}/" compose-workers-windows.yml  # By design... The tag for the worker image is the "overall_version".
sed -i "s/EVENTS_IMAGE/${eventsbroker_version}/" compose-workers-windows.yml
sed -i "s/GRAPHICS_IMAGE/${graphics_version}/" compose-workers-windows.yml
sed -i "s/OVERALL_IMAGE/${overall_version}/" compose-workers-windows.yml

echo ""
echo "The two .yml files have been updated.  Please push them to the repository, if appropriate, or use 'git checkout' to undo the changes."