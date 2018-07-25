#!/bin/bash -e

export HADOOP_USER_NAME=ops_eff
array=( "OPS_STG_EDH_FLIGHT_DELAY_TRANS" "OPS_STG_EDH_FLIGHT_HUB_MASTER" "OPS_STG_EDH_FLIGHT_PLN_REG" "OPS_STG_EDH_FLIGHT_STAT_TRANS" "OPS_STG_EDH_FLT_PLN_ALT_APT" "OPS_STG_EDH_FLT_PLN_RUNWAY" "OPS_STG_EDH_FLT_OUT_TIME_EST" "OPS_STG_EDH_FLT_OFF_TIME_EST" "OPS_STG_EDH_FLT_ON_TIME_EST" "OPS_STG_EDH_FLT_IN_TIME_EST" "OPS_STG_EDH_FLT_COST_INDEX" "OPS_STG_EDH_FLT_PLN_DEP_BAY" "OPS_STG_EDH_FLT_PLN_ARR_BAY" "OPS_STG_EDH_FLT_PLN_DEP_GATE" "OPS_STG_EDH_FLT_PLN_ARR_GATE" "OPS_STG_EDH_FLT_SI_REMARKS_DEP" "OPS_STG_EDH_FLT_SI_REMARKS_ARR" "OPS_STG_EDH_FLIGHT_REMARKS" "OPS_STG_EDH_FLT_DOOR_REMARKS")
DATE=`date +%Y-%m-%d`
echo "table_name,prev_snap_date" | hadoop fs -appendToFile -  $1
for element in ${array[@]}
do
   echo `hadoop fs -rm  -r $2/$element/hx_snapshot_date=$DATE`
    echo "$element,`hadoop fs -ls $2/$element | grep hx_snapshot_date | tail -1 | head -1 | awk -F '/' '{print $(NF)}'| awk -F '=' '{print $NF}'`" | hadoop fs -appendToFile -  $1
done