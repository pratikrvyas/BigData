#!/bin/bash

# Date: 04/09/2018 (MM/DD/YYYY)
# Author: Manu Mukundan
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
# This bash script is developed to run the spark-2 application from oozie by providing all required parameters
# Installation: upload this script (spark-execute.sh) to the workspace /apps/helix/conf/oozie/workspaces/wf_emfgsatelliteplndflightdetailsdaily

unset HADOOP_TOKEN_FILE_LOCATION; 
SPARK_JAR=$1
CLASS_NAME=$2
EXECUTOR_MEMORY=$3
DRIVER_MEMORY=$4
QUEUE=$5
MIN_EXECUTORS=$6
MAX_EXECUTORS=$7
EXECUTOR_CORES=$8
INPUT_DEDUPE_FLIGHTLEVEL_PATH=$9
INPUT_FLIGHT_MASTER_PATH=${10}
INPUT_DEDUPE_ROUTELEVEL_PATH=${11}
INPUT_DEDUPE_WEATHER_PATH=${12}
COMPRESSION=${13}
PARTITIONS=${14}
OUTPUT_PATH=${15}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.kryo.classesToRegister=com.emirates.helix.emfg.udf.UDF --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --conf spark.sql.shuffle.partitions=300 --conf spark.sql.autoBroadcastJoinThreshold=1073741824 --conf spark.kryoserializer.buffer.max=128m --class ${CLASS_NAME} ${SPARK_JAR} -l ${INPUT_DEDUPE_FLIGHTLEVEL_PATH} -f ${INPUT_FLIGHT_MASTER_PATH} -r ${INPUT_DEDUPE_ROUTELEVEL_PATH} -w ${INPUT_DEDUPE_WEATHER_PATH} -i ${OUTPUT_PATH} -c ${COMPRESSION} -p ${PARTITIONS}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.kryo.classesToRegister=com.emirates.helix.emfg.udf.UDF --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --conf spark.sql.shuffle.partitions=300 --conf spark.sql.autoBroadcastJoinThreshold=1073741824 --conf spark.kryoserializer.buffer.max=128m --class ${CLASS_NAME} ${SPARK_JAR} -l ${INPUT_DEDUPE_FLIGHTLEVEL_PATH} -f ${INPUT_FLIGHT_MASTER_PATH} -r ${INPUT_DEDUPE_ROUTELEVEL_PATH} -w ${INPUT_DEDUPE_WEATHER_PATH} -i ${OUTPUT_PATH} -c ${COMPRESSION} -p ${PARTITIONS}