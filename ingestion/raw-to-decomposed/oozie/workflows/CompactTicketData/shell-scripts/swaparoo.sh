#!/bin/bash -e

export HADOOP_USER_NAME=$1
BASE_PATH=$2
HELIX_DATE=$3

hdfs dfs -mv ${BASE_PATH}/${HELIX_DATE} ${BASE_PATH}/_${HELIX_DATE}_deleteme
hdfs dfs -mv ${BASE_PATH}/_${HELIX_DATE} ${BASE_PATH}/${HELIX_DATE}
