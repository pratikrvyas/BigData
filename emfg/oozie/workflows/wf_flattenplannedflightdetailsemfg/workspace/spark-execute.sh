#!/bin/bash

# Date: 09/04/2018 (DD/MM/YYYY)
# Author: Thangamani Kumaravelu
# Project: HELIX
# Team: OpsEfficiency
# This bash script is developed to run the spark-2 application from oozie by providing all required parameters
# Installation: upload this script (spark-execute.sh) to the workspace /apps/helix/conf/oozie/workspaces/wf_fattenplannedflightdetailsemfg

unset HADOOP_TOKEN_FILE_LOCATION; 
SPARK_JAR=$1
CLASS_NAME=$2
EXECUTOR_MEMORY=$3
DRIVER_MEMORY=$4
QUEUE=$5
MIN_EXECUTORS=$6
MAX_EXECUTORS=$7
EXECUTOR_CORES=$8
PLANNEDFLIGHTDTLS_SRC_LOCATION=$9
CONTROL_FILE_SRC_LOCATION=${10}
PLANNEDFLIGHTDTLS_TGT_SERVING_LOCATION=${11}
COALESCE_VALUE=${12}
COMPRESSION=${13}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -i ${PLANNEDFLIGHTDTLS_SRC_LOCATION} -j ${CONTROL_FILE_SRC_LOCATION} -x ${PLANNEDFLIGHTDTLS_TGT_SERVING_LOCATION} -p ${COALESCE_VALUE} -c ${COMPRESSION}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -i ${PLANNEDFLIGHTDTLS_SRC_LOCATION} -j ${CONTROL_FILE_SRC_LOCATION} -x ${PLANNEDFLIGHTDTLS_TGT_SERVING_LOCATION} -p ${COALESCE_VALUE} -c ${COMPRESSION}