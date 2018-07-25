#!/bin/bash

# Date: 04/02/2018
# Author: Manu Mukundan
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
# This bash script is developed to run the spark-2 application from oozie by providing all required parameters
# Installation: upload this script (spark-execute.sh) to the workspace /apps/helix/conf/oozie/workspaces/wf_rawtodecomposed_altealdmdata

unset HADOOP_TOKEN_FILE_LOCATION; 
SPARK_JAR=$1
CLASS_NAME=$2
EXECUTOR_MEMORY=$3
DRIVER_MEMORY=$4
QUEUE=$5
MIN_EXECUTORS=$6
MAX_EXECUTORS=$7

INCR_INPUT_PATH=$8
HIST_INPUT_PATH=$9
COMPRESSION=${10}
LATEST_OUTPUT_PATH=${11}
PARTITIONS=${12}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -i ${INCR_INPUT_PATH} -h ${HIST_INPUT_PATH} -c ${COMPRESSION} -l ${LATEST_OUTPUT_PATH} -p ${PARTITIONS}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -i ${INCR_INPUT_PATH} -h ${HIST_INPUT_PATH} -c ${COMPRESSION} -l ${LATEST_OUTPUT_PATH} -p ${PARTITIONS}
