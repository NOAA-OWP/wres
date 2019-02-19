#!/bin/sh

# Given an environment suffix, discover the entry-point to the cluster at NWCAL
# This script only applies to NWCAL deployments of WRES clusters.
# An environment suffix is a blank string for production, or "-dev", or "-ti".
# The entry point is a CNAME, and this CNAME can change as needed for whatever
# reason (e.g. the machine is dead or the state of the machine is such that the
# machine is effectively dead for WRES purposes). The CNAME of the entry point
# may point to any WRES machine in that environment and it runs the entry point
# software. The remainder of the machines run workers. This script is to figure
# out which machine is currently the actual entry point machine so as to scp or
# ssh commands to that machine.
# The standard output of this script should be the fqdn of the machine currently# acting as the entry point for the given environment. If nothing came back,
# use the above as your troubleshooting guide. An argument such as -dev or -ti
# may be needed for this to work.

# To use this script, a subshell may be handy, e.g.
#   entry_point=$( discover_cluster_entrypoint.sh -dev )

if [ "$1" == "" ]
then
    echo "Assuming production environment because first arg was empty..." >&2
else
    echo "Looking for $1 environment entry point..." >&2
fi

dig +short ***REMOVED***wres${1}.***REMOVED***.***REMOVED*** | head -n 1 | cut -d'.' -f 1-4
