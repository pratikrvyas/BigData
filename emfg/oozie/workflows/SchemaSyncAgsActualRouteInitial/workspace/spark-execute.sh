#!/bin/bash

# Date: 18/02/2018
# Author: Fayaz
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
# This bash script is developed to run the spark-2 application from oozie by providing all required parameters
# Installation: upload this script (spark-execute.sh) to the workspace /apps/helix/conf/oozie/workspaces/wf_schemasyncagsactualrouteinitial

unset HADOOP_TOKEN_FILE_LOCATION; 
SPARK_JAR=$1
CLASS_NAME=$2
EXECUTOR_MEMORY=$3
DRIVER_MEMORY=$4
QUEUE=$5
MIN_EXECUTORS=$6
MAX_EXECUTORS=$7
EXECUTOR_CORES=$8
INPUT_HDFS_PATH_ROUTE=$9
INPUT_HDFS_PATH_REFLINE=${10}
OUTPUT_HDFS_PATH=${11}
COMPRESSION=${12}
PARTITIONS=${13}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -i ${INPUT_HDFS_PATH_ROUTE} -j ${INPUT_HDFS_PATH_REFLINE} -o ${OUTPUT_HDFS_PATH} -c ${COMPRESSION} -p ${PARTITIONS}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -i ${INPUT_HDFS_PATH_ROUTE} -j ${INPUT_HDFS_PATH_REFLINE} -o ${OUTPUT_HDFS_PATH} -c ${COMPRESSION} -p ${PARTITIONS}
