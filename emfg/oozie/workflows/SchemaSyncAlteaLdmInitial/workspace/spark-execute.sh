#!/bin/bash

# Date: 25/Apr/2018
# Author: Thangamani Kumaravelu
# Project: HELIX
# Team: OppEfficiency
# This bash script is developed to run the spark-2 application from oozie by providing all required parameters
# Installation: upload this script (spark-execute.sh) to the workspace 

unset HADOOP_TOKEN_FILE_LOCATION; 
SPARK_JAR=$1
CLASS_NAME=$2
EXECUTOR_MEMORY=$3
DRIVER_MEMORY=$4
QUEUE=$5
MIN_EXECUTORS=$6
MAX_EXECUTORS=$7
INPUT_CLC_FLIGHT_DTLS=$8
INPUT_CLC_LEG_INFO=$9
INPUT_MACS_DESTINATION=${10}
OUTPUT_HDFS_PATH=${11}
SYNC_MAPPING_FILE=${12}
COMPRESSION=${13}
INPUT_FORMAT=${14}
OUTPUT_FORMAT=${15}
PARTITIONS=${16}
EXECUTOR_CORES=${17}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -f ${INPUT_CLC_FLIGHT_DTLS} -l ${INPUT_CLC_LEG_INFO} -d ${INPUT_MACS_DESTINATION} -o ${OUTPUT_HDFS_PATH} -x ${SYNC_MAPPING_FILE} -c ${COMPRESSION} -s ${INPUT_FORMAT} -p ${OUTPUT_FORMAT} -r ${PARTITIONS}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -f ${INPUT_CLC_FLIGHT_DTLS} -l ${INPUT_CLC_LEG_INFO} -d ${INPUT_MACS_DESTINATION} -o ${OUTPUT_HDFS_PATH} -x ${SYNC_MAPPING_FILE} -c ${COMPRESSION} -s ${INPUT_FORMAT} -p ${OUTPUT_FORMAT} -r ${PARTITIONS}