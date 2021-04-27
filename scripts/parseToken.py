#!/usr/bin/env python

import sys
from xml.etree import ElementTree as ET
# Let's ask ET for help!
# parse the input XML file and print the tag, text to a XML output format
# Author: Raymond.Chui@***REMOVED***
# Created: April,2021

if len(sys.argv) < 2:
	print "Usage: " + sys.argv[0] + " inputFile.xml"
	exit(2)
else:
	inputFile = sys.argv[1]

if inputFile.rfind(".xml") < 0:
	print "input file name must with .xml extension!"
	exit(2)

#print "inputFile = " + inputFile
zipFile = inputFile.rstrip(".xml")
#print "zipFile = " + zipFile
tree = ET # an ET tree
xhtml = tree.parse(inputFile)

root = xhtml.getroot()
print "<"+root.tag+">"
for child in root:
	print "<"+child.tag+">"+child.text+"</"+child.tag+">"

print "<filename>"+zipFile+"</filename>"
print "<content_type>application/octet-stream</content_type>"
print "</"+root.tag+">"
