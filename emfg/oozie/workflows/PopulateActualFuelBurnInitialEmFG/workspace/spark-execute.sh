#!/bin/bash

# Date: 28/02/2018
# Author: Fayaz
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
# This bash script is developed to run the spark-2 application from oozie by providing all required parameters
# Installation: upload this script (spark-execute.sh) to the workspace /apps/helix/conf/oozie/workspaces/wf_dedupeagsactualrouteinitial

unset HADOOP_TOKEN_FILE_LOCATION;
export HADOOP_USER_NAME=ops_eff
SPARK_JAR=$1
CLASS_NAME=$2
EXECUTOR_MEMORY=$3
DRIVER_MEMORY=$4
QUEUE=$5
MIN_EXECUTORS=$6
MAX_EXECUTORS=$7
EXECUTOR_CORES=$8
INPUT_DEDUPE_PATH=$9
INPUT_FLIGHT_MASTER_PATH=${10}
OUTPUT_HISTORY_PATH=${11}
OUTPUT_CURRENT_PATH=${12}
OUTPUT_REJECTED_PATH=${13}
COMPRESSION=${14}
PARTITIONS=${15}
LOOK_BACK_MONTHS=${16}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.sql.shuffle.partitions=1500 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -i ${INPUT_DEDUPE_PATH} -j ${INPUT_FLIGHT_MASTER_PATH} -x ${OUTPUT_HISTORY_PATH} -y ${OUTPUT_CURRENT_PATH} -z ${OUTPUT_REJECTED_PATH} -c ${COMPRESSION} -p ${PARTITIONS} -m ${LOOK_BACK_MONTHS}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.sql.shuffle.partitions=1500 --conf spark.sql.autoBroadcastJoinThreshold=1073741824 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -i ${INPUT_DEDUPE_PATH} -j ${INPUT_FLIGHT_MASTER_PATH} -x ${OUTPUT_HISTORY_PATH} -y ${OUTPUT_CURRENT_PATH} -z ${OUTPUT_REJECTED_PATH} -c ${COMPRESSION} -p ${PARTITIONS} -m ${LOOK_BACK_MONTHS}
