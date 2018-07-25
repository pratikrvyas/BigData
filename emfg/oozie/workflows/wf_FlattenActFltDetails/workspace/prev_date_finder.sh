#!/bin/bash -e


export HADOOP_USER_NAME=ops_eff
# array=("flight_pln_time_details" "flight_pln_fuel_details" "flight_pln_distance_details" "flight_pln_waypnt_alternate_details" "flight_arr_weather_message" "flight_dep_weather_message")
array=("OPS_STG_EDH_FLT_ACT_DTLS")
echo "table_name,prev_snap_date" | hadoop fs -appendToFile -  $1
DATE=`date +%Y-%m-%d` 
for element in ${array[@]}
do
   echo `hadoop fs -rm -r $2/$element/hx_snapshot_date=$DATE`
   echo "$element,`hadoop fs -ls $2/$element | grep hx_snapshot_date | tail -1 | head -1 | awk -F '/' '{print $(NF)}'| awk -F '=' '{print $NF}'`" | hadoop fs -appendToFile -  $1
done