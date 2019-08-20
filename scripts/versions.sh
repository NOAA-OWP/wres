#!/bin/bash

# Requires git client installed, expects to be run from the wres top-level
# directory like this: scripts/versions.sh


function get_ver
{
    last_commit_hash=$( git log --format="%h" -n 1 -- $1 build.gradle )
    last_commit_date=$( git log --date=short --format="%cd" -n 1 -- $1 build.gradle )
    last_commit_date_short=$( echo ${last_commit_date} | sed 's/\-//g' - )
    potential_version=${last_commit_date_short}-${last_commit_hash}
    echo ${potential_version}
}

echo Main version: $( get_ver . )

# To use extended globbing in the for loop that follows, extglob.
shopt -s extglob

for directory in wres-*([a-z])
do
    version=$( get_ver ${directory} )
    echo "${directory} version: ${version}"
done
