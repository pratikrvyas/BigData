#!/bin/bash

# Date: 04/02/2018
# Author: Manu Mukundan
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
# This bash script is developed to run the spark-2 application from oozie by providing all required parameters
# Installation: upload this script (spark-execute.sh) to the workspace /apps/helix/conf/oozie/workspaces/wf_decomposedtomodelledaltealdmdata

unset HADOOP_TOKEN_FILE_LOCATION;
export HADOOP_USER_NAME=ops_eff
SPARK_JAR=$1
CLASS_NAME=$2
EXECUTOR_MEMORY=$3
DRIVER_MEMORY=$4
QUEUE=$5
MIN_EXECUTORS=$6
MAX_EXECUTORS=$7

INPUT_HDFS_PATH=$8
OUTPUT_HDFS_PATH=$9
COMPRESSION=${10}
INPUT_FORMAT=${11}
OUTPUT_FORMAT=${12}
PARTITIONS=${13}
OFFSET=${14}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --conf spark.kryo.classesToRegister=com.emirates.helix.emfg.udf.UDF --class ${CLASS_NAME} ${SPARK_JAR} -i ${INPUT_HDFS_PATH} -o ${OUTPUT_HDFS_PATH} -c ${COMPRESSION} -p ${INPUT_FORMAT} -r ${OUTPUT_FORMAT} -t ${PARTITIONS} -d ${OFFSET}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --conf spark.kryo.classesToRegister=com.emirates.helix.emfg.udf.UDF --class ${CLASS_NAME} ${SPARK_JAR} -i ${INPUT_HDFS_PATH} -o ${OUTPUT_HDFS_PATH} -c ${COMPRESSION} -p ${INPUT_FORMAT} -r ${OUTPUT_FORMAT} -t ${PARTITIONS} -d ${OFFSET}
