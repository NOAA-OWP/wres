#!/usr/bin/env python

# Open two files, read them line by line. and write two lines into 3rd file
# Author: Raymond.Chui@***REMOVED***
# Created: February, 2018

file1 = open('metricsValues.txt', 'r')
file2 = open('fileValues.txt', 'r')
file3 = open('joinFiles.txt', 'aw')

while 1:
	line1 = file1.readline()
	if not line1: break
	line1 = line1.strip('\n')
	line2 = file2.readline()
	toArray = line1.split(" ")
	arrayNum = len(toArray)
	lastField = toArray[arrayNum - 1]
	try:
		flastField = float(lastField)
		fline2 = float(line2)
		theDiff = flastField - fline2
		atheDiff = abs(theDiff)
	except:
		print ("last field = ", lastField)
		print ("line2 = ", line2)
		print ("the diff = ", theDiff)
	if atheDiff > 1.0:
		line2 = line2.strip('\n')
		file3.writelines([line1, " <--- Metrics value comparison ----> ", line2, " ---- the difference is  ", str(theDiff), "\n"])
	else:
		file3.writelines([line1, " <--- Metrics value comparison ----> ", line2])

file1.close()
file2.close()
file3.close()
