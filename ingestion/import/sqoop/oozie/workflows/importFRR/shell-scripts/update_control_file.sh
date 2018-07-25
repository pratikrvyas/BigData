#!/bin/bash -e

CONTROL_FILE=$1
echo CONTROL_FILE=${CONTROL_FILE}
echo END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
echo "${END_DATE};SUCCESS" | hdfs dfs -appendToFile - ${CONTROL_FILE}