#!/bin/bash

# Date: 04/Mar/2018
# Author: Pratik Vyas
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
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
INPUT_HDFS_PATH_CLC_FLIGHT_DTLS=$8
INPUT_HDFS_PATH_CLC_CLC_CABIN_SEC_INFO=$9
INPUT_HDFS_PATH_CLC_CREW_FIGURES=${10}
INPUT_HDFS_PATH_CLC_LEG_INFO=${11}
INPUT_HDFS_PATH_CLC_PAX_BAG_TOTAL=${12}
INPUT_HDFS_PATH_CLC_PAX_FIGURES=${13}
INPUT_HDFS_PATH_MACS_LOAD_SHEET_DATA=${14}
INPUT_HDFS_PATH_MACS_CARGO_DSTR_CMPT=${15}
OUTPUT_HDFS_PATH=${16}
COALESCE_VALUE=${17}
COMPRESSION=${18}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -a ${INPUT_HDFS_PATH_CLC_FLIGHT_DTLS} -b ${INPUT_HDFS_PATH_CLC_CLC_CABIN_SEC_INFO} -c ${INPUT_HDFS_PATH_CLC_CREW_FIGURES} -d ${INPUT_HDFS_PATH_CLC_LEG_INFO} -e ${INPUT_HDFS_PATH_CLC_PAX_BAG_TOTAL} -f ${INPUT_HDFS_PATH_CLC_PAX_FIGURES} -g ${INPUT_HDFS_PATH_MACS_LOAD_SHEET_DATA} -h ${INPUT_HDFS_PATH_MACS_CARGO_DSTR_CMPT} -o ${OUTPUT_HDFS_PATH} -p ${COALESCE_VALUE} -q ${COMPRESSION}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -a ${INPUT_HDFS_PATH_CLC_FLIGHT_DTLS} -b ${INPUT_HDFS_PATH_CLC_CLC_CABIN_SEC_INFO} -c ${INPUT_HDFS_PATH_CLC_CREW_FIGURES} -d ${INPUT_HDFS_PATH_CLC_LEG_INFO} -e ${INPUT_HDFS_PATH_CLC_PAX_BAG_TOTAL} -f ${INPUT_HDFS_PATH_CLC_PAX_FIGURES} -g ${INPUT_HDFS_PATH_MACS_LOAD_SHEET_DATA} -h ${INPUT_HDFS_PATH_MACS_CARGO_DSTR_CMPT} -o ${OUTPUT_HDFS_PATH} -p ${COALESCE_VALUE} -q ${COMPRESSION} 

