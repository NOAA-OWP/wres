#!/usr/bin/env python

import sys
from HTMLParser import HTMLParser

# This python script provides three different HTML parser classes BuiltHistoryParser, BuiltRevisionParser and BuiltRecentParser
# Each class parser build_history.html, build_revision.html and build_recent.html file, respectively. 
# Author: Raymond.Chui@***REMOVED***
# Created: December, 2017
# Input File: one of HTML file mentioned above 

class BuiltHistoryParser(HTMLParser):
	def handle_starttag(self, tag, attrs):
		if (tag == 'a'): # print the open tag <a> with
			#print("Start tag:", tag)
			for attr in attrs:
				if (attr[0] == 'href'): # print the attribute href only
					if (attr[1].find('lastSuccessfulBuild') >= 0):
						print(" attr:", attr[0], attr[1]) # array 0 is name and 1 is value
	def handle_data(self, data):
		if (data.find('Last successful build') >= 0):
			print("Data:", data) # get the built #number

class BuiltRevisionParser(HTMLParser):
	def handle_data(self, data):
		if (data.find('Revision') >= 0):
			print("Data:", data) # get the revision number
		if (data.find(': ') == 0):
			print("Data:", data) # get the revision number

class BuiltRecentParser(HTMLParser):
	def handle_starttag(self, tag, attrs):
		if (tag == 'a'): # print the open tag <a> with
			#print("Start tag:", tag) 
			for attr in attrs:
				if (attr[0] == 'href'): # print the attribute href only
					if (attr[1].find('.zip') >= 0):
						print(" attr:", attr[0], attr[1]) # array 0 is name and 1 is value
	def handle_data(self, data):
		if (data.find('.zip') >= 0):
			print("Data:", data) # get the built file name 

class BuiltRecentSystestParser(HTMLParser):
	def handle_data(self, data):
		if (data.find('AM') >= 0 or data.find('PM') >= 0 or data.find('.zip') >= 0):
			if (len(data) >= 22):
				if (data.find('.zip') >= 0):
					print("Data:", data), # print filename.zip without \n
				else:
					print("Data:", data) # print the date, time with \n 

# ./searchInstallBuild.py systests_recentAll.html | grep Data | gawk -F "'" '{print($4,$8)}' | tr -d "," | gawk '{print($4,$2,$3,$5,$6,$1)}' > unsortrecentsystest.txt 
# ./formatLine.py unsortrecentsystest.txt | grep -v Invalid | sort | tail -1 | cut -d' ' -f2
#print sys.argv
#print len(sys.argv)

if len(sys.argv) < 2:
	print "Usage: " + sys.argv[0] + " inputFile.html"
	exit(2)
else:
	inputFile = sys.argv[1]

print "Input file = " + inputFile

if inputFile == "build_revision_wres-vis.html":
	parser = BuiltRevisionParser()
elif inputFile == "build_history_wres-vis.html":
	parser = BuiltHistoryParser()
elif inputFile == "build_recent_wres-vis.html" or inputFile == "systests_recent_wres-vis.html":
	parser = BuiltRecentParser()
elif inputFile == "systests_recentAll.html":
	parser = BuiltRecentSystestParser()
else:
	print "Unrecognized file name " + inputFile
	exit(2) 


parser.feed(open(inputFile).read())
parser.close() 
