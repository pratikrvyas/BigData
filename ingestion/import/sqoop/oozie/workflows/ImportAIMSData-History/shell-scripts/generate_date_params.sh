#!/bin/bash -e



CONTROL_FILE=$1
AIMS_STATUS_FILE_LOOKUP=$2

echo CONTROL_FILE=${CONTROL_FILE}
echo PRC_DATE=`hdfs dfs -cat  ${CONTROL_FILE} | tail -1|cut -d";" -f2 `
PRC_DATE=`hdfs dfs -cat  ${CONTROL_FILE} | tail -1|cut -d";" -f2 `
echo EXECUTION_DATE=`date +"%Y-%m-%d"`
#echo END_DATE=$(date +"%Y-%m-01 %H:%M:%S" -d "$START_DATE") 
#echo START_DATE=`tail -n 1 ${CONTROL_FILE} | cut -d";" -f2`
#echo END_DATE=`date +"%Y-%m-%d %H:%M:%S"`


prc_start_date=$(date  +"%Y-%m-01" -d "$PRC_DATE")
START_DATE=$(date  +"%Y-%m-%d" -d "$prc_start_date 1 month")
int_end_date=$(date  +"%Y-%m-%d" -d "$prc_start_date 2 month")
END_DATE=$(date +"%Y-%m-%d" -d "$int_end_date -1 days")

echo prc_start_date=$(date  +"%Y-%m-01" -d "$PRC_DATE")
echo START_DATE=$(date  +"%Y-%m-%d" -d "$prc_start_date 1 month")
echo int_end_date=$(date  +"%Y-%m-%d" -d "$prc_start_date 2 month")
echo END_DATE=$(date +"%Y-%m-%d" -d "$int_end_date -1 days")

#echo EXECUTION_DATE=`date +"%Y-%m-01"`
echo AIMS_STATUS_FILE_LOOKUP=`hdfs dfs -tail ${AIMS_STATUS_FILE_LOOKUP}| tail -1|cut -d";" -f1`   
echo HELIX_TIMESTAMP=`date -u +"%s"`

if [ "${AIMS_STATUS_FILE_LOOKUP}" ==  "F" ]; then
	echo HELIX_RUN_IMPORT="TRUE" 

else
	echo HELIX_RUN_IMPORT="FALSE"
fi