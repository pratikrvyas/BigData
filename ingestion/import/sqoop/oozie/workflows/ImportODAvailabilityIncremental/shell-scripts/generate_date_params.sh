#!/bin/bash -e
CONTROL_FILE=$1
echo CONTROL_FILE=${CONTROL_FILE}
LAST_END_DATE=`tail -n 1 ${CONTROL_FILE} | cut -d" " -f2`
START_DATE=$(date +%Y-%m-%d -d "$LAST_END_DATE + 1 day")
echo START_DATE="$START_DATE"
echo END_DATE=`date --date="1 day ago" '+%Y-%m-%d'`
#echo END_DATE=2017-04-23