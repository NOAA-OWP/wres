#!/bin/bash

if [ $# -lt 1 ]
then
	echo "Usage: $0 sorted_pairs.csv"
	exit 2
fi
cat $1 | gawk -F, '{if(NF > 6) {print($1"," $2"," $3"," $4"," $5"," $7)} else print($0)}'
