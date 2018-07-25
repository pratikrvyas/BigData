#!/bin/bash -e

echo HELIX_DATE=`date -u +"%Y-%m-%d"`

if [ `date -u +"%-d"` -lt 3 ]
then
	echo "HELIX_RUN_IMPORT=FALSE"
else
	echo "HELIX_RUN_IMPORT=TRUE" 
fi