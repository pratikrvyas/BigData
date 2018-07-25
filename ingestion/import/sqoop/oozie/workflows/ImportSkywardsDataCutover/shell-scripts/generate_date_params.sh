#!/bin/bash -e

CONTROL_FILE=$1

echo CONTROL_FILE=${CONTROL_FILE}
echo END_DATE=`date +"%Y-%m-%d"`
echo START_DATE=`tail -n 1 ${CONTROL_FILE} | cut -d";" -f2`

if [ `date +"%-H"` -lt 2 ]
then
	echo "HELIX_RUN_IMPORT=FALSE"
else
	echo "HELIX_RUN_IMPORT=TRUE" 
fi
