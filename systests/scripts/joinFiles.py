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
	line2 = line2.strip('\n')
	toArray = line1.split(" ")
	arrayNum = len(toArray)
	lastField = toArray[arrayNum - 1]
	try:
		# str to float
		flastField = float(lastField)
		fline2 = float(line2)
		# around to 6th precision after the decimal point
		rflastField = round(flastField, 6)
		rfline2 = round(fline2, 6)
		# get the difference
		theDiff = rflastField - rfline2
		# get the absolute value
		atheDiff = abs(theDiff)
		#if atheDiff > 1.0:
		# if they are not the same
		# if rflastField != rfline2:
		if atheDiff > 0.000001:
			file3.writelines([line1, " <--- Metrics value comparison ----> ", line2, " ---- the difference is  ", str(theDiff), "\n"])
		else:
			file3.writelines([line1, " <--- Metrics value comparison ----> ", line2, "\n"])
	except:
		print ("last field = ", lastField)
		print ("line2 = ", line2)
		#print ("the diff = ", theDiff)

file1.close()
file2.close()
file3.close()
