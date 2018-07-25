#!/bin/bash

# Date: 17/May/2018
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
PATH_TO_EGDS_PREREQUISITE=$8
PATH_TO_FLIGHT_MASTER_CURRENT=$9
PATH_TO_TAXI_FUEL_FLOW=${10}
TIME_PERIOD=${11}
COALESCE_VALUE=${12}
COMPRESSION=${13}
PATH_TO_AGS=${14}
PATH_TO_ALTEA=${15}
PACKAGES=${16}
NUMBER_OF_MONTHS=${17}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} \
--driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} \
--conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} \
--packages ${PACKAGES}  \
--class ${CLASS_NAME} ${SPARK_JAR} \
-a ${PATH_TO_FLIGHT_MASTER_CURRENT} \
-b ${PATH_TO_EGDS_PREREQUISITE} \
-c ${PATH_TO_TAXI_FUEL_FLOW} \
-e ${PATH_TO_AGS} \
-f ${PATH_TO_ALTEA} \
-g ${TIME_PERIOD}  \
-h ${NUMBER_OF_MONTHS}  \
-i ${COALESCE_VALUE}  \
-j ${COMPRESSION}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} \
--driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} \
--conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} \
--jars ${PACKAGES}  \
--class ${CLASS_NAME} ${SPARK_JAR} \
-a ${PATH_TO_FLIGHT_MASTER_CURRENT} \
-b ${PATH_TO_EGDS_PREREQUISITE} \
-c ${PATH_TO_TAXI_FUEL_FLOW} \
-e ${PATH_TO_AGS} \
-f ${PATH_TO_ALTEA} \
-g ${TIME_PERIOD}  \
-h ${NUMBER_OF_MONTHS}  \
-i ${COALESCE_VALUE}  \
-j ${COMPRESSION}