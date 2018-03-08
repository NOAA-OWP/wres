#!/bin/bash
#
# Run the R script Metrics for each scenario benchmark sorted_pairs.csv
# and output the results to testMetricsResults.txt
# Author: Raymond.Chui@***REMOVED***
# Created: Feb., 2018

#if [ ! -f sorted_pairs.csv ]
#then
#	echo "file sorted_pairs.csv doesn't exist."
#	exit
#fi
if [ $# -gt 0 ]
then
	ID=$1
	listFile="$ID"_dirListing.txt
	sortedFile="$ID"_sorted_pairs.csv
	resultFile="ID"_testMetricsResults
else
	listFile=dirListing.txt
	sortedFile=sorted_pairs.csv
	resultFile=testMetricsResults
fi


#CSV_FILES=`ls *.csv | grep -v pairs`
#CSV_FILES=`grep .csv dirListing.txt | grep -v pairs`
#CSV_FILES=`grep .csv dirListing.txt | grep -v pairs | egrep -v '(CONTINGENCY_TABLE|FREQUENCY_BIAS|PEIRCE_SKILL_SCORE|QUANTILE_QUANTILE_DIAGRAM|HEFS_RELIABILITY_DIAGRAM|HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE|HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM|HEFS_RANK_HISTOGRAM|HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE|HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE)'`
#CSV_FILES=`grep .csv $listFile | grep -v pairs | egrep -v '(CONTINGENCY_TABLE|FREQUENCY_BIAS|PEIRCE_SKILL_SCORE|QUANTILE_QUANTILE_DIAGRAM|HEFS_RELIABILITY_DIAGRAM|HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE|HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM|HEFS_RANK_HISTOGRAM|HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE|HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE)'`
CSV_FILES=`grep .csv $listFile | grep -v pairs | egrep -v '(CONTINGENCY_TABLE|FREQUENCY_BIAS|PEIRCE_SKILL_SCORE|DIAGRAM|ERRORS_BY_OBSERVED_VALUE|ERRORS_BY_FORECAST_VALUE|CONTINUOUS_RANKED_PROBABILITY|HISTOGRAM|EQUITABLE|PROBABILITY|THREAT)'`
echo "csv files = $CSV_FILES"
for CSV_FILE in $CSV_FILES
do
	#echo $CSV_FILE

	numberOfRows=`cat $CSV_FILE | wc -l`
	numberOfColumns=`head -1 $CSV_FILE | gawk -F, '{print NF}'`
	#echo $numberOfColumns
	# start at column 5
	column=5
	errorFile="$CSV_FILE"_error.txt
	#touch $errorFile
	cat /dev/null > $errorFile

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
			#Rscript ../../rsrc/CheckMetrics.R sorted_pairs.csv "$ename" $value 2>&1 | tee -a testMetricsResults.txt
			#(Rscript ../../rsrc/CheckMetrics.R sorted_pairs.csv "$ename" $value > temp1.txt) 2>> $errorFile
			#if [ -f checkedSorted_pairs.csv ]
			if [ -f $sortedFile ]
			then
				#(Rscript ../../rsrc/CheckMetrics.R "$ID"_sorted_pairs.csv "$ename" $value > temp1.txt) 2>> $errorFile
				(Rscript ../../rsrc/CheckMetrics.R $sortedFile "$ename" $value > temp1.txt) 2>> $errorFile
				#(Rscript ../../rsrc/CheckMetrics.R checkedSorted_pairs.csv "$ename" $value > temp1.txt) 2>> $errorFile
			#else
			#	(Rscript ../../rsrc/CheckMetrics.R sorted_pairs.csv "$ename" $value > temp1.txt) 2>> $errorFile
			fi
			#(Rscript ../../rsrc/CheckMetrics.R sorted_pairs.csv "$ename" $value > temp1.txt) 2>> $errorFile
			head -6 temp1.txt > header.txt
			sed -n "7,$"p temp1.txt | gawk '{print($1 "          "  $3)}' > metricsValues.txt
			#rm -v temp1.txt
		fi

		# compare the output of metrics values versus the *.csv file 
		row=2 # start at row 2
		cat /dev/null > fileValues.txt
		while [ $row -le $numberOfRows ]
		do
			#echo -n "Cell $row,$column ------------ " 2>&1 | tee -a testMetricsResults.txt
			# print the field value at row,column in CSV_FILE
			#sed -n "$row"p $CSV_FILE | gawk -F, -v column=$column '{print $column}' 2>&1 | tee -a testMetricsResults.txt
			sed -n "$row"p $CSV_FILE | gawk -F, -v column=$column '{print $column}' >> fileValues.txt 
			row=`expr $row + 1` # next row
		done 
		if [ -f joinFiles.txt ]
		then
			rm -v joinFiles.txt
		fi
		pwd > joinFiles.txt
		echo "$CSV_FILE" >> joinFiles.txt
		cat header.txt >> joinFiles.txt
		../../scripts/joinFiles.py # join metricsValues.txt and fileValues.txt append to joinFiles.txt
		#cat joinFiles.txt >> testMetricsResults.txt
		#cat joinFiles.txt | tee -a testMetricsResults.txt
		cat joinFiles.txt | tee -a $resultFile 
		echo "" | tee -a $resultFile 
		# next column
		column=`expr $column + 1`
	done
	if [ ! -s $errorFile ] # if error file size is not empty, remove it
	then
		rm -v $errorFile
	fi
done
