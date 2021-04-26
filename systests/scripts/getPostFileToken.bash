#!/bin/bash

# Copy the two JUnitLog and graphicsLog text files and zip them
# then parse their tokens and output to an output XML file
# Finally upload that two zip files to Redmine with curl command
# Author: Raymond.Chui@***REMOVED***
# Created: April, 2021

if [ $# -lt 3 ]
then
	echo "Usage: $0 JUnitLog_filename graphicsLog_filename outputFile.xml"
	exit 2
fi
JUnitLog_FILENAME=$1
graphicsLog_FILENAME=$2
outputFile=$3
echo "JUnitLog_FILENAME = $JUnitLog_FILENAME"
echo "graphicsLog_FILENAME = $graphicsLog_FILENAME"
echo "outputFile = " $outputFile
#exit

if [ -f ${JUnitLog_FILENAME} ]
then
	JUnitLog_FILENAME_Base=`/bin/basename ${JUnitLog_FILENAME}`
	cp -v ${JUnitLog_FILENAME} $JUnitLog_FILENAME_Base 
	ls -l $JUnitLog_FILENAME_Base
	JUnitLog_FILENAME_BaseZip=${JUnitLog_FILENAME_Base}.zip
	/bin/zip ${JUnitLog_FILENAME_BaseZip} ${JUnitLog_FILENAME_Base}
	ls -l ${JUnitLog_FILENAME_BaseZip}
	if [ -f ${JUnitLog_FILENAME_BaseZip}.xml ]
	then
#		ls ${JUnitLog_FILENAME_Base}.xml
		rm -v ${JUnitLog_FILENAME_BaseZip}.xml
	fi
#	echo -n "$JUnitLog_FILENAME_BaseZip and " >> $outputFile
fi

if [ -f ${graphicsLog_FILENAME} ]
then
	graphicsLog_FILENAME_Base=`/bin/basename ${graphicsLog_FILENAME}`
	cp -v ${graphicsLog_FILENAME} $graphicsLog_FILENAME_Base  
	ls -l $graphicsLog_FILENAME_Base
	graphicsLog_FILENAME_BaseZip=${graphicsLog_FILENAME_Base}.zip
	/bin/zip ${graphicsLog_FILENAME_BaseZip} ${graphicsLog_FILENAME_Base}
	ls -l ${graphicsLog_FILENAME_BaseZip}
	if [ -f ${graphicsLog_FILENAME_BaseZip}.xml ]
	then
#		ls ${graphicsLog_FILENAME_Base}.xml
		rm -v ${graphicsLog_FILENAME_BaseZip}.xml
	fi
#	echo "${graphicsLog_FILENAME_BaseZip}" >> $outputFile
fi
#exit
echo "<uploads type=\"array\">" >> $outputFile 
# get JUnitLog token
if [ -f ${JUnitLog_FILENAME_BaseZip} ]
then
	curl -H 'X-Redmine***REMOVED***: ***REMOVED***' --data-binary "@${JUnitLog_FILENAME_BaseZip}" -H "Content-Type: application/octet-stream" -X POST https://***REMOVED***/redmine/uploads.xml?filename=${JUnitLog_FILENAME_BaseZip} -o ${JUnitLog_FILENAME_BaseZip}.xml -v

	if [ -s ${JUnitLog_FILENAME_BaseZip}.xml ]
	then
	# Parse the token to the XML file
		/wres_share/releases/install_scripts/parseToken.py ${JUnitLog_FILENAME_BaseZip}.xml >> $outputFile
	fi
fi
# get graphicsLog token
if [ -f ${graphicsLog_FILENAME_BaseZip} ]
then
	curl -H 'X-Redmine***REMOVED***: ***REMOVED***' --data-binary "@${graphicsLog_FILENAME_BaseZip}" -H "Content-Type: application/octet-stream" -X POST https://***REMOVED***/redmine/uploads.xml?filename=${graphicsLog_FILENAME_BaseZip} -o ${graphicsLog_FILENAME_BaseZip}.xml -v

	if [ -s ${graphicsLog_FILENAME_BaseZip}.xml ]
	then
	# Parse the token to the XML file
		/wres_share/releases/install_scripts/parseToken.py ${graphicsLog_FILENAME_BaseZip}.xml >> $outputFile
	fi
fi
echo "</uploads>" >> $outputFile
echo "</issue>" >> $outputFile

cat $outputFile

#exit
/usr/bin/curl -v -x '' -H 'X-Redmine***REMOVED***: ***REMOVED***' https://***REMOVED***/redmine/issues/89538.xml -X PUT -H 'Content-Type: application/xml' -d "@${outputFile}"


#ls -l ${JUnitLog_FILENAME_Base} ${graphicsLog_FILENAME_Base}
rm -v ${JUnitLog_FILENAME_Base} ${graphicsLog_FILENAME_Base}
#ls -l ${JUnitLog_FILENAME_BaseZip} ${graphicsLog_FILENAME_BaseZip}
rm -v ${JUnitLog_FILENAME_BaseZip} ${graphicsLog_FILENAME_BaseZip}
#ls -l ${JUnitLog_FILENAME_BaseZip}.xml ${graphicsLog_FILENAME_BaseZip}.xml
rm -v ${JUnitLog_FILENAME_BaseZip}.xml ${graphicsLog_FILENAME_BaseZip}.xml
