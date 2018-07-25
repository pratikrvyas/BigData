#!/bin/bash -e

CONTROL_FILE=$1
START_DATE=$2
END_DATE=$3
HDFS_PATH=$4
CURRENT_TIMESTAMP=`date -u +"%s"`

echo CONTROL_FILE=${CONTROL_FILE}
echo START_DATE=${START_DATE}
echo END_DATE=${END_DATE}

echo "${START_DATE};${END_DATE}" > ${CONTROL_FILE}.${CURRENT_TIMESTAMP}
hdfs dfs -appendToFile ${CONTROL_FILE}.${CURRENT_TIMESTAMP} ${HDFS_PATH}/${CONTROL_FILE}
