

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
PAYLOAD_HISTORY_SCHEMA_SYNC_PATH=$8
PAYLOAD_LOADSHEET_INCREMENTAL_PATH=$9
PAYLOAD_LOADSHEET_DEDUPE_HISTORY_LOCATION=${10}
PAYLOAD_LOADSHEET_DEDUPE_CURRENT_LOCATION=${11}
COALESCE_VALUE=${12}
LOOKBACK_MONTHS=${13}
DATA_LOAD_MODE=${14}
COMPRESSION=${15}



echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -i ${PAYLOAD_HISTORY_SCHEMA_SYNC_PATH} -j ${PAYLOAD_LOADSHEET_INCREMENTAL_PATH} -x ${PAYLOAD_LOADSHEET_DEDUPE_HISTORY_LOCATION} -y ${PAYLOAD_LOADSHEET_DEDUPE_CURRENT_LOCATION} -p ${COALESCE_VALUE} -m ${LOOKBACK_MONTHS} -n ${DATA_LOAD_MODE} -c ${COMPRESSION}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} -i ${PAYLOAD_HISTORY_SCHEMA_SYNC_PATH} -j ${PAYLOAD_LOADSHEET_INCREMENTAL_PATH} -x ${PAYLOAD_LOADSHEET_DEDUPE_HISTORY_LOCATION} -y ${PAYLOAD_LOADSHEET_DEDUPE_CURRENT_LOCATION} -p ${COALESCE_VALUE} -m ${LOOKBACK_MONTHS} -n ${DATA_LOAD_MODE} -c ${COMPRESSION}
