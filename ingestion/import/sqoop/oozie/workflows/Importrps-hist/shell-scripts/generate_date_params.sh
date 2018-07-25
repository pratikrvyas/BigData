#!/bin/bash -e

BATCH_FILE=$1

echo START_DATE=`date +"%Y-%m-%d %H:%M:%S"`

echo POS_SCTR_TRGT=`hdfs dfs -text ${BATCH_FILE}/* | awk -F ";" '{ print $2 }' | sed -n "/CBI_FRPS_POS_SCTR_TRGT_REV/p"` 
echo OD_TRGT_REV=`hdfs dfs -text ${BATCH_FILE}/* | awk -F ";" '{ print $2 }' | sed -n "/CBI_FRPS_OD_TRGT_REV/p"` 
echo TRGT_EXHG_RTES=`hdfs dfs -text ${BATCH_FILE}/* | awk -F ";" '{ print $2 }' | sed -n "/CBI_FRPS_TRGT_EXHG_RTES/p"` 
echo LEG_TRGT_REV=`hdfs dfs -text ${BATCH_FILE}/* | awk -F ";" '{ print $2 }' | sed -n "/CBI_FRPS_RT_LEG_TRGT_REV/p"`  

echo BATCH_DATA=`hdfs dfs -text ${BATCH_FILE}/* | wc -l`  



#CONTROL_FILE=$1
#HDFS_CONTROL_FILE_KEY_LOOKUP=$2
#
#echo CONTROL_FILE=${CONTROL_FILE}
#echo HDFS_CONTROL_FILE_KEY_LOOKUP=${HDFS_CONTROL_FILE_KEY_LOOKUP}
#
#echo END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
#echo START_DATE=`hdfs dfs -cat ${CONTROL_FILE}| tail -1| cut -d";" -f2`
#echo HELIX_TIMESTAMP=`date -u +"%s"`
#echo  BATCH_ID=`hdfs dfs -tail ${HDFS_CONTROL_FILE_KEY_LOOKUP}`
#hdfs dfs -text hdfs://helixcloud//apps/helix/conf/oozie/workspaces/wf_importrps/rps_batch_details/* | awk -F ";" '{ print $1 }' | sed -n "/CBI_FRPS_POS_SCTR_TRGT_REV/p"