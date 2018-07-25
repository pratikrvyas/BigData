#!/bin/bash -e
export HADOOP_USER_NAME=scv_ops
IBE_CONTROL_FILE=$1
END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
START_DATE=`hdfs dfs -cat ${IBE_CONTROL_FILE}| tail -1| cut -d";" -f2`
echo HELIX_TIMESTAMP=`date -u +"%s"` 
echo IBE_CONTROL_FILE=${IBE_CONTROL_FILE}
echo START_DATE=${START_DATE}
echo END_DATE=${END_DATE}
echo "${START_DATE};${END_DATE}" | hdfs dfs -appendToFile - ${IBE_CONTROL_FILE}