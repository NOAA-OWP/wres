#!/bin/bash
#
# Run the R script Metrics for each scenario benchmark sorted_pairs.csv
# and output the results to testMetricsResults.txt
# Author: Raymond.Chui@***REMOVED***
# Created: Feb., 2018

if [ ! -f sorted_pairs.csv ]
then
	echo "file sorted_pairs.csv doesn't exist."
	exit
fi

CSV_FILES=`ls *.csv | grep -v pairs`
for CSV_FILE in $CSV_FILES
do
	#echo $CSV_FILE

	numberOfRows=`cat $CSV_FILE | wc -l`
	numberOfColumns=`head -1 $CSV_FILE | gawk -F, '{print NF}'`
	#echo $numberOfColumns
	# start at column 5
	column=5

	while [ $column -le $numberOfColumns ]
	do
		# get the name/value pairs from the 1st line with a comma separator and convert "All data" to NA
		nameValuePairs=`head -1 $CSV_FILE | gawk -F, -v column=$column '{print $column}' | sed -e 's/All data/ \>\= NA/'`
		#echo $nameValuePairs
		# get the name in 1st field with a greater than sign as separator and convert to all lower case
		name=$(echo $nameValuePairs | gawk -F'>' '{print $1}' | tr [A-Z] [a-z])
		# evaluated the name
		ename=`eval echo -n "$name"`
		# get the value from 2nd field with a greater than sign as separator, relacce the equal sign to null.
		value=$(echo $nameValuePairs | sed -e 's/\=//' | cut -d'>' -f2 | gawk '{print $1}')
		#echo $value
		if [ -f sorted_pairs.csv ] # just double check
		then
			Rscript ../../rsrc/CheckMetrics.R sorted_pairs.csv "$ename" $value 2>&1 | tee -a testMetricsResults.txt
		fi

		# compare the output of metrics values versus the *.csv file 
		row=2 # start at row 2
		while [ $row -le $numberOfRows ]
		do
			echo -n "Cell $row,$column ------------ " 2>&1 | tee -a testMetricsResults.txt
			# print the field value at row,column in CSV_FILE
			sed -n "$row"p $CSV_FILE | gawk -F, -v column=$column '{print $column}' 2>&1 | tee -a testMetricsResults.txt
			row=`expr $row + 1` # next row
		done 
		# next column
		column=`expr $column + 1`
	done
done
