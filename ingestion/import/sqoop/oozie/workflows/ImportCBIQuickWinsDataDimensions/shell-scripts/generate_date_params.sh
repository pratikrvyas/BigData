#!/bin/bash -e

CONTROL_FILE=$1
CBI_STATUS_FILE_LOOKUP_PATH=$2

echo CONTROL_FILE=${CONTROL_FILE}
echo END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
echo START_DATE=`tail -n 1 ${CONTROL_FILE} | cut -d";" -f2`

CBI_STATUS_FILE_LOOKUP=`hdfs dfs -tail ${CBI_STATUS_FILE_LOOKUP_PATH}`

echo CBI_STATUS_FILE_LOOKUP=${CBI_STATUS_FILE_LOOKUP}

if [[ "${CBI_STATUS_FILE_LOOKUP}" -eq "0" ]]; then
        echo HELIX_RUN_IMPORT="TRUE"
else
        echo HELIX_RUN_IMPORT="FALSE"
fi
