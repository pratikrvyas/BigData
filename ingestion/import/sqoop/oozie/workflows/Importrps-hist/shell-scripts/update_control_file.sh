#!/bin/bash -e
export HADOOP_USER_NAME=scv_ops
START_DATE=$1
BATCH_DETAILS_FILE=$2
CONTROL_FILE=$3

END_DATE=`date +"%Y-%m-%d %H:%M:%S"`

echo  END_DATE=$END_DATE

BATCH_DATA1=`hdfs dfs -text $BATCH_DETAILS_FILE/* | wc -l`

if [ $BATCH_DATA1 == 0 ]; then

echo "${START_DATE};${END_DATE};" | hdfs dfs -appendToFile - ${CONTROL_FILE}
else
hdfs dfs -cat $BATCH_DETAILS_FILE/* |  while read line;
do

echo "${START_DATE};${END_DATE};${line}" | hdfs dfs -appendToFile - ${CONTROL_FILE}

done
fi