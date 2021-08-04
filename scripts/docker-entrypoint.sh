#!/bin/sh
# In order for worker-created files to be deleted by tasker, need group write,
# and in order for the java process to successfully set group write permissions,
# need more lenient umask.
umask 0002
# Re-use the current process so that signals are trapped properly: see #87813
exec bin/wres-worker /usr/bin/wres
