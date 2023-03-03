#!/usr/bin/env python

import sys
import json
import re

if len(sys.argv) < 2:
	print "Usage: " + sys.argv[0] + " inputFile.json"
	exit(2)
else:
	inputFile = sys.argv[1]

f = open(inputFile)
data = json.load(f)

# Scan the JSON data for the action associated with the BuildData _class.
# We can pull the evision identifier from that.  None will be printed 
# if its not found.  
actions = data['actions']
for action in actions:
    if not (action.get('_class') is None):
        if action.get('_class') == "hudson.plugins.git.util.BuildData":
            revisionNumber = action['lastBuiltRevision']['SHA1']

print revisionNumber

core_zip_name="UNKNOWN"
systest_zip_name="UNKNOWN"

# Look for the core .zip, which matches "wres-#", where # is a digit.
# This assumes the first part of the revision identifier is the date.
for artifact in data['artifacts']:
    if re.match( "^wres\-[0-9]+" , artifact.get('fileName')):
        core_zip_name = artifact.get('fileName')
        break

# Look for the systests .zip, which is an artifact with a file name
# that starts with "systests". Easier.
for artifact in data['artifacts']:
    if artifact.get('fileName').startswith('systests'):
        systests_zip_name = artifact.get('fileName')
        break

# Look for the wres vis .zip, which is an artifact with a file name
# that starts with "wres-vis". Easier.
for artifact in data['artifacts']:
    if artifact.get('fileName').startswith('wres-vis'):
        wresvis_zip_name = artifact.get('fileName')
        break

print core_zip_name
print systests_zip_name
print wresvis_zip_name
f.close() 
