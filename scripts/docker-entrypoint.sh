#!/bin/sh
# In order for worker-created files to be deleted by tasker, need group write,
# and in order for the java process to successfully set group write permissions,
# need more lenient umask.
umask 0002
bin/wres-worker /usr/bin/wres
