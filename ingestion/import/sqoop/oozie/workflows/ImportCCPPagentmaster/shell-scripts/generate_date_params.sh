#!/bin/bash -e
export HADOOP_USER_NAME=scv_ops

CONTROL_FILE=$1

echo CONTROL_FILE=${CONTROL_FILE}

echo END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
echo START_DATE=`hdfs dfs -cat ${CONTROL_FILE}| tail -1| cut -d";" -f2`
echo HELIX_TIMESTAMP=`date -u +"%s"`
