#!/bin/bash

IDS=$(cat dirListing.txt | grep csv | grep -v pairs | cut -d'_' -f1 | sort | uniq)
echo $IDS > IDFile.txt

for ID in $IDS
do
	grep $ID dirListing.txt | grep csv > "$ID"_dirListing.txt
	#cat sorted_pairs.csv | gawk -F, '{if(NF > 6) {print($1"," $2"," $3"," $4"," $5"," $6) 
	grep $ID sorted_pairs.csv | gawk -F, '{if(NF > 6) {print($1"," $2"," $3"," $4"," $5"," $6) 
                                print($1"," $2"," $3"," $4"," $5"," $7)} 
                        else print($0)}' > "$ID"_sorted_pairs.csv 
done
