#!/bin/bash -e

CONTROL_FILE=$1
SQOOP_PATH=$2

echo CONTROL_FILE=${CONTROL_FILE}

echo END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
echo START_DATE=`tail -n 1 ${CONTROL_FILE} | cut -d";" -f2`
echo HELIX_DATE=`echo $SQOOP_PATH | rev | cut -d '/' -f2 | rev`
echo HELIX_TIME=`echo $SQOOP_PATH | rev | cut -d '/' -f1 | rev`
echo HELIX_TIMESTAMP=`date -u +"%s"`

if [ `date +"%-H"` -eq 0 ] && [ `date +"%-M"` -lt 35 ]
then
	echo "HELIX_RUN_IMPORT=TRUE"
elif [ `date +"%-H"` -lt 2 ]
then
	echo "HELIX_RUN_IMPORT=FALSE"
else
	echo "HELIX_RUN_IMPORT=TRUE" 
fi
