#!/bin/bash

# Date: 05/15/2018 (MM/DD/YYYY)
# Author: Krishnaraj Rajagopal
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
# This bash script is developed to run the spark-2 application from oozie by providing all required parameters

unset HADOOP_TOKEN_FILE_LOCATION; 
SPARK_JAR=$1
CLASS_NAME=$2
EXECUTOR_MEMORY=$3
DRIVER_MEMORY=$4
QUEUE=$5
MIN_EXECUTORS=$6
MAX_EXECUTORS=$7
EXECUTOR_CORES=$8
FLIGHT_MASTER_SRC_LOCATION=$9
FUELMON_DEDUPE_LOCATION=${10}
AGS_SRC_LOCATION=${11}
COMPRESSION=${12}
ULTRAMAIN_DEDUPE_LOCATION=${13}
MFP_DEDUPE_LOCATION=${14}
EPIC_DEDUPE_LOCATION=${15}
MASTER_EPIC_REJECTED_LOCATION=${16}
LOOK_BACK_MONTHS=${17}
OUTPUT_HISTORY_LOCATION=${18}
OUTPUT_CURRENT_LOCATION=${19}
DATA_LOAD_MODE=${20}
COALESCE_VALUE=${21}


echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --conf spark.sql.shuffle.partitions=300 --conf spark.sql.autoBroadcastJoinThreshold=1073741824 --conf spark.kryoserializer.buffer.max=128m --class ${CLASS_NAME} ${SPARK_JAR} --flight_master_src_location ${FLIGHT_MASTER_SRC_LOCATION} --fuelmon_dedupe_location ${FUELMON_DEDUPE_LOCATION} --ags_src_location ${AGS_SRC_LOCATION} --compression ${COMPRESSION} --ultramain_dedupe_location ${ULTRAMAIN_DEDUPE_LOCATION} --mfp_dedupe_location ${MFP_DEDUPE_LOCATION} --epic_dedupe_location ${EPIC_DEDUPE_LOCATION} --master_epic_rejected_location ${MASTER_EPIC_REJECTED_LOCATION} --look_back_months ${LOOK_BACK_MONTHS} --output_history_location ${OUTPUT_HISTORY_LOCATION} --output_current_location ${OUTPUT_CURRENT_LOCATION} --data_load_mode ${DATA_LOAD_MODE} --coalesce_value ${COALESCE_VALUE}

spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --conf spark.sql.shuffle.partitions=300 --conf spark.sql.autoBroadcastJoinThreshold=1073741824 --conf spark.kryoserializer.buffer.max=128m --class ${CLASS_NAME} ${SPARK_JAR} --flight_master_src_location ${FLIGHT_MASTER_SRC_LOCATION} --fuelmon_dedupe_location ${FUELMON_DEDUPE_LOCATION} --ags_src_location ${AGS_SRC_LOCATION} --compression ${COMPRESSION} --ultramain_dedupe_location ${ULTRAMAIN_DEDUPE_LOCATION} --mfp_dedupe_location ${MFP_DEDUPE_LOCATION} --epic_dedupe_location ${EPIC_DEDUPE_LOCATION} --master_epic_rejected_location ${MASTER_EPIC_REJECTED_LOCATION} --look_back_months ${LOOK_BACK_MONTHS} --output_history_location ${OUTPUT_HISTORY_LOCATION} --output_current_location ${OUTPUT_CURRENT_LOCATION} --data_load_mode ${DATA_LOAD_MODE} --coalesce_value ${COALESCE_VALUE}