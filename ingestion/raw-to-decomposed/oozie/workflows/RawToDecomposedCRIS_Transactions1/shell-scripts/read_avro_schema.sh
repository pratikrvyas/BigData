#!/bin/bash -e

export HADOOP_USER_NAME=scv_ops

echo "AVRO_SCHEMA=`hdfs dfs -cat $1 | tr -d '\n\r'`"