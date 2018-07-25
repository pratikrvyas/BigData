#!/bin/bash -e

CONTROL_FILE=$1

echo CONTROL_FILE=${CONTROL_FILE}
echo END_DATE= $(date --date "-0 month" +%Y-%m-01)

START_DATE=`tail -n 1 ${CONTROL_FILE} | cut -d";" -f2`
echo START_DATE=$START_DATE

# START_DATE= $(date -d "$PREV_DATE" +%'Y-%m-01')
# echo $START_DATE

echo HELIX_TIMESTAMP=`date -u +"%s"`