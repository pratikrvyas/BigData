#!/bin/bash -e

export HADOOP_USER_NAME=scv_ops
CONTROL_FILE=$1
START_DATE=$2
END_DATE=$3
HDFS_PATH=$4
CURRENT_TIMESTAMP=`date -u +"%s"`

echo CONTROL_FILE=${CONTROL_FILE}
echo START_DATE=${START_DATE}
echo END_DATE=${END_DATE}
echo CURRENT_TIMESTAMP=${CURRENT_TIMESTAMP}
echo HDFS_PATH=${HDFS_PATH}

echo "${START_DATE};${END_DATE}" > ${CONTROL_FILE}.${CURRENT_TIMESTAMP}
hadoop fs -appendToFile ${CONTROL_FILE}.${CURRENT_TIMESTAMP} ${HDFS_PATH}/${CONTROL_FILE}
