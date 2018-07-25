#!/bin/bash -e


export HADOOP_USER_NAME=ops_eff
array=("ops_stg_edh_pln_flt_distwaypnt" "ops_stg_edh_pln_flt_fuelwaypnt")
echo "table_name,prev_snap_date" | hadoop fs -appendToFile -  $1
DATE=`date +%Y-%m-%d` 
for element in ${array[@]}
do
   echo `hadoop fs -rm -r $2/$element/hx_snapshot_date=$DATE`
   echo "$element,`hadoop fs -ls $2/$element | grep hx_snapshot_date | tail -1 | head -1 | awk -F '/' '{print $(NF)}'| awk -F '=' '{print $NF}'`" | hadoop fs -appendToFile -  $1
done