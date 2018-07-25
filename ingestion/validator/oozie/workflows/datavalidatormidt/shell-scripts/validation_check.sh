#!/bin/bash

# -----------------------------------------------
#
#       Developer:         Satish Reddy (S751237)
#       Purpose:         metrics_validation.sh 
#
# -----------------------------------------------

VALIDATION_FILE=$1
CONTROL_FILE=$2
export HADOOP_USER_NAME=scv_ops

execution_date=`date +"%y%m%d%H%M%S"`
validationstatus=`hdfs dfs -cat $VALIDATION_FILE/* | sed -n '/ERROR/p' | awk -F "," '{ print $7 }'`
batch_id=`hdfs dfs -cat $VALIDATION_FILE/* | sed -n '/MIDT_PNR/p' | awk -F "," '{ print $3 }' | xargs`
processingdate=`hdfs dfs -cat $VALIDATION_FILE/* | sed -n '/MIDT_PNR/p' | awk -F "," '{ print $4 }'`

  if [ -z $validationstatus]
                     then
                            
							  echo "$batch_id$processingdate;$execution_date" | hdfs dfs -appendToFile - ${CONTROL_FILE}
						
                     else
                              echo FLAG="ERROR"    
                     fi
