#!/bin/bash -e

# export HADOOP_USER_NAME=scv_ops

CONTROL_FILE=$1
NUMBER_OF_MONTHS=$2

echo CONTROL_FILE=${CONTROL_FILE}
echo NUMBER_OF_MONTHS=${NUMBER_OF_MONTHS}

echo START_DATE=`hdfs dfs -cat ${CONTROL_FILE}| tail -1| cut -d";" -f2`
START_DATE=`hdfs dfs -cat ${CONTROL_FILE}| tail -1| cut -d";" -f2`
echo END_DATE=$(date +"%Y-%m-%d %H:%M:%S" -d "$START_DATE $NUMBER_OF_MONTHS month")
echo HELIX_TIMESTAMP=`date -u +"%s"`
