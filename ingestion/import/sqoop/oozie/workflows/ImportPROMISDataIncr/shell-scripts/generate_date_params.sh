#!/bin/bash -e

export HADOOP_USER_NAME=scv_ops

CONTROL_FILE=$1
HDFS_CONTROL_FILE_DATE_LOOKUP=$2
PROMIS_STATUS_FILE_LOOKUP=$3

echo CONTROL_FILE=${CONTROL_FILE}
echo HDFS_CONTROL_FILE_DATE_LOOKUP=${HDFS_CONTROL_FILE_DATE_LOOKUP}
echo PROMIS_STATUS_FILE_LOOKUP=${PROMIS_STATUS_FILE_LOOKUP}

echo START_DATE=`hdfs dfs -cat  ${CONTROL_FILE} | tail -1|cut -d";" -f2`
#echo START_DATE=`tail -n 1 ${CONTROL_FILE} | cut -d";" -f2`
echo END_DATE =`hdfs dfs -tail ${HDFS_CONTROL_FILE_DATE_LOOKUP}| tail -1|cut -d";" -f1`
echo PROMIS_STATUS_LOOKUP=`hdfs dfs -tail ${PROMIS_STATUS_FILE_LOOKUP}| tail -1|cut -d";" -f1`   
echo HELIX_TIMESTAMP=`date -u +"%s"`

if [ "${PROMIS_STATUS_LOOKUP}" == "C" ]; then
	echo HELIX_RUN_IMPORT="TRUE" 

else
	echo HELIX_RUN_IMPORT="FALSE"
fi