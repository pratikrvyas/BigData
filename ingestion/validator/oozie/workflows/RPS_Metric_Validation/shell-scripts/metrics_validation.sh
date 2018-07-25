#!/bin/bash

# -----------------------------------------------
#
#       Developer:         Satish Reddy (S751237)
#       Purpose:         metrics_validation.sh 
#
# -----------------------------------------------


VALIDATION_FILE=$1
INTERFACE_ID=$2
METRIC=$3
export HADOOP_USER_NAME=scv_ops
#VALIDATION_FILE='/user/satishs/1'
#INTERFACE_ID='CBI_RPS_OD_TRGT_REV'
#METRIC='FNL_PAX'

echo MEASURE_COLUMN=$METRIC
echo INTERFACE_ID=$INTERFACE_ID
echo TRGT_MEASURE_VALUE=`hdfs dfs -cat $VALIDATION_FILE/* | sed -n "/$INTERFACE_ID,$METRIC/p" | awk -F "," '{ print $5 }'`
echo VALIDATION_STATUS=`hdfs dfs -cat $VALIDATION_FILE/* | sed -n "/$INTERFACE_ID,$METRIC/p" | awk -F "," '{ print $6 }'`
echo BATCH_ID=`hdfs dfs -cat $VALIDATION_FILE/* | sed -n "/$INTERFACE_ID,$METRIC/p" | awk -F "," '{ print $3 }'`