#!/bin/bash

# Requires git client installed, expects to be run from the wres top-level
# directory like this: scripts/versions.sh


# Accepts any number of directories arguments, e.g. of modules.
# Returns the most recent WRES version auto-detected from git.
# When the Dockerfile has uncommitted modifications, flag "-dev" (dirty).
function get_ver
{
    last_commit_hash=$( git log --format="%h" -n 1 -- build.gradle $@ )
    last_commit_date=$( git log --date=iso8601 --format="%cd" -n 1 -- build.gradle $@ )
    last_commit_date_utc=$( date --date "${last_commit_date}" --iso-8601 --utc )
    last_commit_date_short=$( echo ${last_commit_date_utc} | sed 's/\-//g' - )
    potential_version=${last_commit_date_short}-${last_commit_hash}

    # Look for the git status of the dockerfile for the directory passed
    dockerfile_modified=$( git status --porcelain -- $1/Dockerfile | grep "^ M" )

    if [ "$dockerfile_modified" != "" ]
    then
        echo ${potential_version}-dev
    else
	echo ${potential_version}
    fi
}

echo Main version: $( get_ver . )

# To use extended globbing in the for loop that follows, extglob.
shopt -s extglob

for directory in wres-*([a-z])
do
    # For those with zips, we want to change versions when dependencies change.
    if [[ "$directory" == "wres-tasker" || "$directory" == "wres-worker" ]]
    then
        version=$( get_ver ${directory} "wres-messages" )
    elif [[ "$directory" == "wres-vis" ]]
    then
        version=$( get_ver ${directory} "wres-datamodel" \
                           "wres-util" "wres-eventsbroker" "wres-events" )
    else
        version=$( get_ver ${directory} )
    fi

    echo "${directory} version: ${version}"
done
