#!/usr/bin/env python

import sys
import json

if len(sys.argv) < 2:
	print "Usage: " + sys.argv[0] + " inputFile.json"
	exit(2)
else:
	inputFile = sys.argv[1]

f = open(inputFile)
data = json.load(f)

buildNumber = data['lastSuccessfulBuild']['number']

print buildNumber

f.close() 
