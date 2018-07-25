#!/bin/bash

# Date: 18/Mar/2018 
# Author: Pratik Vyas
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
# This bash script is developed to run the spark-2 application from oozie by providing all required parameters
# Installation: upload this script (spark-execute.sh) to the workspace /apps/helix/conf/oozie/workspaces/wf_populateemfgsatelliteflightpayload

# Date: 01/May/2018 
# Author: Fayaz Shaik
# Added the LDM parameters

export HADOOP_USER_NAME=ops_eff 
unset HADOOP_TOKEN_FILE_LOCATION; 
SPARK_JAR=$1
CLASS_NAME=$2
EXECUTOR_MEMORY=$3
DRIVER_MEMORY=$4
QUEUE=$5
MIN_EXECUTORS=$6
MAX_EXECUTORS=$7
EXECUTOR_CORES=$8
INPUT_LOADSHEET_DEDUPE_LOCATION=$9
INPUT_LIDOFSUM_DEDUPE_LOCATION=${10}
INPUT_LDM_DEDUPE_LOCATION=${11}
INPUT_FLIGHTMASTER_LOCATION=${12}
OUTPUT_PAYLOAD_HISTORY_LOCATION=${13}
OUTPUT_PAYLOAD_CURRENT_LOCATION=${14}
REJECTED_LOADSHEET_LOCATION=${15}
REJECTED_LIDOFSUM_LOCATION=${16}
REJECTED_LDM_LOCATION=${17}
COALESCE_VALUE=${18}
LOOKBACK_MONTHS=${19}
DATA_LOAD_MODE=${20}
COMPRESSION=${21}

echo spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.sql.autoBroadcastJoinThreshold=1073741824 --conf spark.sql.shuffle.partitions=50 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} --payload_src_loadsheet_dedupe_location ${INPUT_LOADSHEET_DEDUPE_LOCATION} --lidofsumflightlevel_src_dedupe_location ${INPUT_LIDOFSUM_DEDUPE_LOCATION} --ldm_src_dedupe_location ${INPUT_LDM_DEDUPE_LOCATION} --flightmaster_location ${INPUT_FLIGHTMASTER_LOCATION} --payload_tgt_history_location ${OUTPUT_PAYLOAD_HISTORY_LOCATION} --payload_tgt_current_location ${OUTPUT_PAYLOAD_CURRENT_LOCATION} --payload_tgt_loadsheet_rejected_location ${REJECTED_LOADSHEET_LOCATION} --lidofsumflightlevel_tgt_rejected_location ${REJECTED_LIDOFSUM_LOCATION} --ldm_tgt_rejected_location ${REJECTED_LDM_LOCATION} --coalesce_value ${COALESCE_VALUE} --lookback_months ${LOOKBACK_MONTHS} --data_load_mode ${DATA_LOAD_MODE} --compression ${COMPRESSION}


spark2-submit --master yarn --deploy-mode cluster --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --executor-cores ${EXECUTOR_CORES} --queue ${QUEUE} --conf spark.sql.autoBroadcastJoinThreshold=1073741824 --conf spark.sql.shuffle.partitions=50 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} --class ${CLASS_NAME} ${SPARK_JAR} --payload_src_loadsheet_dedupe_location ${INPUT_LOADSHEET_DEDUPE_LOCATION} --lidofsumflightlevel_src_dedupe_location ${INPUT_LIDOFSUM_DEDUPE_LOCATION} --ldm_src_dedupe_location ${INPUT_LDM_DEDUPE_LOCATION} --flightmaster_location ${INPUT_FLIGHTMASTER_LOCATION} --payload_tgt_history_location ${OUTPUT_PAYLOAD_HISTORY_LOCATION} --payload_tgt_current_location ${OUTPUT_PAYLOAD_CURRENT_LOCATION} --payload_tgt_loadsheet_rejected_location ${REJECTED_LOADSHEET_LOCATION} --lidofsumflightlevel_tgt_rejected_location ${REJECTED_LIDOFSUM_LOCATION} --ldm_tgt_rejected_location ${REJECTED_LDM_LOCATION} --coalesce_value ${COALESCE_VALUE} --lookback_months ${LOOKBACK_MONTHS} --data_load_mode ${DATA_LOAD_MODE} --compression ${COMPRESSION}