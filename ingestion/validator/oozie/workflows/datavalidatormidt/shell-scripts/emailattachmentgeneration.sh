#!/bin/bash
export HADOOP_USER_NAME=scv_ops
TGPATH=$1
hdfs dfs -cat $TGPATH/* | hdfs dfs -put - $TGPATH/validation_results.txt