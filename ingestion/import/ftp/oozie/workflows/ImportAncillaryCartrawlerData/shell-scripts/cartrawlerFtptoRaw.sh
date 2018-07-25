#!/bin/bash

#export HADOOP_USER_NAME=$1
export HADOOP_USER_NAME=scv_ops
RUN_DATE=$1
HDFS_PATH=$2
#HDFS_PATH=/user/sandeeps/CarTrawler
#LOCAL_PATH='/data/2/helix/cartrawler'
LOCAL_PATH='/mnt/helix/cartrawler'
FTP_HOST=ftp1.emirates.com
FTP_USER=FTHLXCTLWR01
FTP_PASSWORD='d8DafR$TraqA'
FTP_DIR=CARTRAWLER
WORKSPACE_PATH='/user/hue/oozie/workspaces/ws_ImportAncillaryCartrawlerData'

# extract date from the HDFS target path, substract one day and put it in the right format
#TODAY=$(date -d "$date -0 days" +'%Y_%m_%d')
TODAY=$(date -d "$RUN_DATE -0 days" +'%Y-%m-%d')
PATH_DATE=`echo $TODAY`
#FILE_DATE=`date "--date=${PATH_DATE} -0 day" +%Y%m%d`
FILE_DATE=`date "--date=${RUN_DATE}" +%Y_%m_%d`
FILE_DATE_PREV=`date "--date=${RUN_DATE} -0 day" +%Y_%m_%d`
HDFS_PATH_DIR=`date "--date=${RUN_DATE}" +%Y-%m-%d`

CARTRAWLER_PAX_DROP_FILE_CSV=cartrawler_pax_drop_${FILE_DATE}.csv
CARTRAWLER_AIRPORT_TRANSFER_CSV=cartrawler_airport_transfer_${FILE_DATE}.csv
LOG_FILE_PAX_DROP=cartrawler_pax_drop_${FILE_DATE}.log
LOG_FILE_AIRPORT_TRANSFER=cartrawler_airport_transfer_${FILE_DATE}.log
FTP_SUCCESS_FILE_NAME=_SUCCESS_${FILE_DATE}
#FTP_SUCCESS_FILE=_SUCCESS_${FILE_DATE}
##FTP_SUCCESS_FILE=_SUCCESS_${FILE_DATE}.csv
LOG_FTP_SUCCESS_FILE=FTP_SUCCESS_FILE_${FILE_DATE}.log
FTP_LOCAL_SUCCESS_FILE=LOAD_TO_HADOOP_DIR_PATH_RES_${FILE_DATE}.log
FTP_SUCCESS_FILE_GREP=`ls -l | grep "$FTP_SUCCESS_FILE_NAME *"`
FTP_SUCCESS_FILE=`echo "$FTP_SUCCESS_FILE_GREP"|awk -F " " '{print $9}'`;
# -------------------------------------------------------
cd ${LOCAL_PATH}
# ---Check whether the processed data for given data is available in hilix server or not.
LOCAL_FILES_COUNT_HELIX_SERVER=`ls -l | grep "${FILE_DATE}.csv$" | wc -l`
if [ ${LOCAL_FILES_COUNT_HELIX_SERVER} -eq 3 ]
then
 echo "RESULT_OUTCOME=Files for the ${RUN_DATE} are already transferred from FTP to Helix server"
 else
# -- remove existing files, if present
LOCAL_FILES_COUNT=`ls -l | grep ^- | wc -l`
if [ ${LOCAL_FILES_COUNT} -gt 0 ]
then
 rm ${LOCAL_PATH}/*
 #echo "${LOCAL_FILES_COUNT} Total LOCAL_FILES_COUNT"
 echo ${LOCAL_PATH}
fi
wget -i "ftp://$FTP_USER:$FTP_PASSWORD@$FTP_HOST/${FTP_DIR}/${FTP_SUCCESS_FILE}" 2>&1 | tee ${LOG_FTP_SUCCESS_FILE}
cat ${LOG_FTP_SUCCESS_FILE} | grep 'saved'
if [ $? -eq 0 ]
then
# -------------------------------------------------------
# -- download file from FTP

wget -i "ftp://$FTP_USER:$FTP_PASSWORD@$FTP_HOST/${FTP_DIR}/${CARTRAWLER_PAX_DROP_FILE_CSV}" 2>&1 | tee ${LOG_FILE_PAX_DROP}
wget -i "ftp://$FTP_USER:$FTP_PASSWORD@$FTP_HOST/${FTP_DIR}/${CARTRAWLER_AIRPORT_TRANSFER_CSV}" 2>&1 | tee ${LOG_FILE_AIRPORT_TRANSFER}
paste ${LOG_FILE_AIRPORT_TRANSFER} ${LOG_FILE_PAX_DROP} > ${FTP_LOCAL_SUCCESS_FILE} #####
SAVED_COUNT=`grep -o 'saved' ${FTP_LOCAL_SUCCESS_FILE} | wc -l` ######
if [ ${SAVED_COUNT} -eq 2 ] ######
then
	if [ -s ${LOCAL_PATH}/${CARTRAWLER_PAX_DROP_FILE_CSV} ] && [ -s ${LOCAL_PATH}/${CARTRAWLER_AIRPORT_TRANSFER_CSV} ]
	then echo "RESULT_OUTCOME=${CARTRAWLER_PAX_DROP_FILE_CSV} and ${CARTRAWLER_AIRPORT_TRANSFER_CSV} files downloaded successfully"
	echo ${TODAY} | hdfs dfs -appendToFile - /user/hue/oozie/workspaces/ws_ImportAncillaryCartrawlerData_UAT/importDateCheck.txt
	else
		echo "RESULT_OUTCOME=${CARTRAWLER_PAX_DROP_FILE_CSV} and ${CARTRAWLER_AIRPORT_TRANSFER_CSV} files could not be downloaded successfully, check log for more details"
		exit 1;
	fi
else
	echo "RESULT_OUTCOME=${CARTRAWLER_PAX_DROP_FILE_CSV} could not be downloaded successfully, check log for more details"
	exit 1;
fi

# -------------------------------------------------------
# create target directory and upload file into HDFS
 hdfs dfs -mkdir -p ${HDFS_PATH}/${HDFS_PATH_DIR}
# hdfs dfs -copyFromLocal ${LOCAL_PATH}/cartrawler_pax_drop_${FILE_DATE}.csv ${LOCAL_PATH}/cartrawler_airport_transfer_${FILE_DATE}.csv ${HDFS_PATH}/${HDFS_PATH_DIR}
hdfs dfs -copyFromLocal -f ${LOCAL_PATH}/cartrawler_pax_drop_${FILE_DATE}.csv ${LOCAL_PATH}/cartrawler_airport_transfer_${FILE_DATE}.csv ${HDFS_PATH}/${HDFS_PATH_DIR}
# -------------------------------------------------------
# -- create and upload success indicator into HDFS
touch ${LOCAL_PATH}/_SUCCESS
#hdfs dfs -copyFromLocal ${LOCAL_PATH}/_SUCCESS ${HDFS_PATH}/${HDFS_PATH_DIR}
hdfs dfs -copyFromLocal -f ${LOCAL_PATH}/_SUCCESS ${HDFS_PATH}/${HDFS_PATH_DIR}
else
  echo "RESULT_OUTCOME=${FTP_SUCCESS_FILE} Success file not found Check whether files uploaded in FTP location "
exit 1;
  fi
  fi
