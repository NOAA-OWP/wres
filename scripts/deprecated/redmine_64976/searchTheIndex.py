#!/usr/bin/env python

import sys
from HTMLParser import HTMLParser

# This Python script is called by getNplace_DStore_data.bash script within the same directory
# It parses and handles the <a> tag and its href attribute values in two types of HTML file,
# one type is seached the directory name in yyyymmdd_index.html, and other type is seached
# range file names in xxxx_range_index.html
#
# Author:	Raymond.Chui@***REMOVED***
# Created:	June, 2019
# Called by:	getNplace_DStore_data.bash


class SearchTodayIndexParser(HTMLParser):
	def handle_starttag(self, tag, attrs):
		if (tag == 'a'): # search for <a> tag
			for attr in attrs:
				if (attr[0] == 'href'): # search for href attribute only
					if (attr[1].startswith(data_type) and attr[1].find(data_type) >= 0):
						print (attr[1].rstrip('/')), # right strip the '/' and print no \n (new line)
					else:
						continue

class SearchRangeIndexParser(HTMLParser):
	def handle_starttag(self, tag, attrs):
		if (tag == 'a'): # search for <a> tag
			for attr in attrs:
				if (attr[0] == 'href'): # search for href attribute only
					if ((attr[1].find('channel_rt') >= 0) and (attr[1].endswith('.nc') == True)):
						print attr[1] 

if len(sys.argv) < 4:
	print "Usage: " + sys.argv[0] + " inputFile.html data_type yyyymmdd"
	exit(2)
else:
	inputFile = sys.argv[1]
	data_type = sys.argv[2]
	yyyymmdd = sys.argv[3]

#print inputFile
#print data_type

if inputFile.find(data_type) >= 0 and  inputFile.endswith('_index.html'):
	parser = SearchRangeIndexParser()
elif (inputFile == yyyymmdd + '_index.html'):
	parser = SearchTodayIndexParser()
else:
	print "Unrecognized file name " + inputFile
	exit(2)

parser.feed(open(inputFile).read())
parser.close()
