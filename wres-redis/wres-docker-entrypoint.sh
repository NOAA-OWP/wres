#!/bin/sh

# This script is called with the same arguments that are used when calling
# the default, Redis entry point script. Those arguments will be passed
# through to that entrypoint script, below.

echo "=========="
echo "Executing redis-check-aof to clean up the AOF manifest..."
yes | /usr/local/bin/redis-check-aof --fix /data/appendonlydir/appendonly.aof.manifest
echo "Execution of redis-check-aof is complete. See above for logging."
echo "=========="

echo "Running the default entrypoint script with arguments '$*' ..."
echo ""
/usr/local/bin/docker-entrypoint.sh $*

