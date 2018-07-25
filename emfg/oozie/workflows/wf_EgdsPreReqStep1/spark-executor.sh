#!/bin/bash

# Date: 18/Apr/2018
# Author: Oleg Baydakov
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
PATH_TO_EGDS_INCREMENTAL=$8
PATH_TO_FLIGHT_MASTER_CURRENT=$9
PATH_TO_EDGS_PREREQUISITE=${10}
PATH_TO_EDGS_REJECTED=${11}
TIME_PERIOD=${12}
COALESCE_VALUE=${13}
COMPRESSION=${14}
DATE_FOR_FOLDER=${15}

# current date
date_for_folder=$(date -d "yesterday" '+%Y-%m-%d')
#  one hour ago for snaptime
# date_for_folder="2018-04-21"

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} \
--driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} \
--conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} \
--jars spark-avro_2.11-4.0.0.jar \
--class ${CLASS_NAME} ${SPARK_JAR} -a ${PATH_TO_EGDS_INCREMENTAL} \
-b ${PATH_TO_FLIGHT_MASTER_CURRENT} -c ${PATH_TO_EDGS_PREREQUISITE} \
-d ${PATH_TO_EDGS_REJECTED} -e ${TIME_PERIOD} -f ${COALESCE_VALUE} -g ${DATE_FOR_FOLDER} -h ${COMPRESSION}


echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} \
--driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} \
--conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} \
--packages  com.databricks:spark-avro_2.11:4.0.0   \
--class ${CLASS_NAME} ${SPARK_JAR} -a ${PATH_TO_EGDS_INCREMENTAL} \
-b ${PATH_TO_FLIGHT_MASTER_CURRENT} -c ${PATH_TO_EDGS_PREREQUISITE} \
-d ${PATH_TO_EDGS_REJECTED} -e ${TIME_PERIOD} -f ${COALESCE_VALUE}   -g ${DATE_FOR_FOLDER} -h ${COMPRESSION}