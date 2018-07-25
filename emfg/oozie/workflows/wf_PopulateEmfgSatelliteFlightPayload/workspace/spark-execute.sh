#!/bin/bash

# Date: 18/Mar/2018 
# Author: Pratik Vyas
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
# This bash script is developed to run the spark-2 application from oozie by providing all required parameters
# Installation: upload this script (spark-execute.sh) to the workspace /apps/helix/conf/oozie/workspaces/wf_populateemfgsatelliteflightpayload

unset HADOOP_TOKEN_FILE_LOCATION; 
SPARK_JAR=$1
CLASS_NAME=$2
EXECUTOR_MEMORY=$3
DRIVER_MEMORY=$4
QUEUE=$5
MIN_EXECUTORS=$6
MAX_EXECUTORS=$7
EXECUTOR_CORES=$8
INPUT_PAYLOAD_SRC_LOADSHEET_DEDUPE_LOCATION=$9
INPUT_LIDOFSUMFLIGHTLEVEL_SRC_DEDUPE_LOCATION=${10}
INPUT_FLIGHTMASTER_CURRENT_LOCATION=${11}
OUTPUT_PAYLOAD_TGT_HISTORY_LOCATION=${12}
OUTPUT_PAYLOAD_TGT_CURRENT_LOCATION=${13}
OUTPUT_PAYLOAD_TGT_LOADSHEET_REJECTED_LOCATION=${14}
OUTPUT_LIDOFSUMFLIGHTLEVEL_TGT_REJECTED_LOCATION=${15}
COALESCE_VALUE=${16}
LOOKBACK_MONTHS=${17}
COMPRESSION=${18}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} --payload_src_loadsheet_dedupe_location ${INPUT_PAYLOAD_SRC_LOADSHEET_DEDUPE_LOCATION} --lidofsumflightlevel_src_dedupe_location ${INPUT_LIDOFSUMFLIGHTLEVEL_SRC_DEDUPE_LOCATION} --flightmaster_current_location ${INPUT_FLIGHTMASTER_CURRENT_LOCATION} --payload_tgt_history_location ${OUTPUT_PAYLOAD_TGT_HISTORY_LOCATION} --payload_tgt_current_location ${OUTPUT_PAYLOAD_TGT_CURRENT_LOCATION} --payload_tgt_loadsheet_rejected_location ${OUTPUT_PAYLOAD_TGT_LOADSHEET_REJECTED_LOCATION} --lidofsumflightlevel_tgt_rejected_location ${OUTPUT_LIDOFSUMFLIGHTLEVEL_TGT_REJECTED_LOCATION} --coalesce_value ${COALESCE_VALUE} --lookback_months ${LOOKBACK_MONTHS} --compression ${COMPRESSION}


spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} --payload_src_loadsheet_dedupe_location ${INPUT_PAYLOAD_SRC_LOADSHEET_DEDUPE_LOCATION} --lidofsumflightlevel_src_dedupe_location ${INPUT_LIDOFSUMFLIGHTLEVEL_SRC_DEDUPE_LOCATION} --flightmaster_current_location ${INPUT_FLIGHTMASTER_CURRENT_LOCATION} --payload_tgt_history_location ${OUTPUT_PAYLOAD_TGT_HISTORY_LOCATION} --payload_tgt_current_location ${OUTPUT_PAYLOAD_TGT_CURRENT_LOCATION} --payload_tgt_loadsheet_rejected_location ${OUTPUT_PAYLOAD_TGT_LOADSHEET_REJECTED_LOCATION} --lidofsumflightlevel_tgt_rejected_location ${OUTPUT_LIDOFSUMFLIGHTLEVEL_TGT_REJECTED_LOCATION} --coalesce_value ${COALESCE_VALUE} --lookback_months ${LOOKBACK_MONTHS} --compression ${COMPRESSION}
