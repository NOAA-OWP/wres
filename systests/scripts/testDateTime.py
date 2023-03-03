#!/usr/bin/env python

# This Python script is compare the difference from thr argument provide datetime with
# current datetime. If more than 1 hour differences, then print the number of days and hours
# differences. Otherwise, just print the argument provided datatime
# Author: <a href="mailto:Raymond.Chui@***REMOVED***" />
# Created: May 15th, 2019


import sys
import datetime

if len(sys.argv) < 2:
        print "Usage: " + sys.argv[0] + " %Y-%m-%d %H:%M:%S"
        exit(2)
else:
        inputDateTime = sys.argv[1]

d = datetime.datetime.strptime(inputDateTime, "%Y-%m-%d %H:%M:%S")
nowDateTime = datetime.datetime.utcnow()
#print d.strftime("%Y%j%H%M")
#print nowDateTime.strftime("%Y%j%H%M")
# where %j is the number of days in a year from 1 to 365/366

# This delta is from datetime.timedelta(days[,seconds[, microseconds[, ...) class
delta = nowDateTime - d
#print delta

# print delta days and hours, since datetime.timedelta class only has three attributes days, seconds, and
# microseconds.
hours =  delta.seconds // 3600 # return an interger of left decimal
remainder = delta.seconds % 3600 # return the remaider of right decimal
minutes = remainder // 60
seconds = remainder % 60
if delta.days > 0 or hours > 0:
	print ((delta.days * 24) + hours)
else:
	print hours 
#if delta.days > 0 or hours > 0 or minutes > 30:
#    print delta.days,"days and",hours,"hours",minutes,"minutes",seconds,"seconds ago|down"
