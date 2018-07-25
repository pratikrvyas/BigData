#!/bin/bash

export HADOOP_USER_NAME=$1
HDFS_PATH=$2
LOCAL_PATH='/data/1/share/google-analytics'
FTP_HOST=ftp.emirates.com
FTP_USER=FTHelx01
FTP_PASSWORD=Proact1ve
FTP_DIR=google-analytics

# extract date from the HDFS target path, substract one day and put it in the right format
PATH_DATE=`echo $HDFS_PATH | rev | cut -d '/' -f1 | rev`
FILE_DATE=`date "--date=${PATH_DATE} -1 day" +%Y%m%d`

BOOKINGS_FILE_ZIP=bookings_hits_${FILE_DATE}.gz
BOOKINGS_FILE_TXT=bookings_hits_${FILE_DATE}
LOG_FILE=bookings_hits_${FILE_DATE}.log

# -------------------------------------------------------
# -- remove existing files, if present
if [ -f ${LOCAL_PATH}/${BOOKINGS_FILE_ZIP} ]
then 
	rm ${LOCAL_PATH}/${BOOKINGS_FILE_ZIP}
fi

if [ -f ${LOCAL_PATH}/${BOOKINGS_FILE_TXT} ]
then 
	rm ${LOCAL_PATH}/${BOOKINGS_FILE_TXT}
fi

# -------------------------------------------------------
# -- download file from FTP
cd ${LOCAL_PATH}
wget -i "ftp://$FTP_USER:$FTP_PASSWORD@$FTP_HOST/${FTP_DIR}/${BOOKINGS_FILE_ZIP}" 2>&1 | tee ${LOG_FILE}
cat ${LOG_FILE} | grep 'saved'
if [ $? -eq 0 ]
then
	if [ -s ${LOCAL_PATH}/${BOOKINGS_FILE_ZIP} ]
	then echo "${BOOKINGS_FILE_ZIP} has been downloaded successfully"
	else
		echo "${BOOKINGS_FILE_ZIP} could not be downloaded successfully or is a zerobyte file, check log for more details"
		exit 1;
	fi
else
	echo "${BOOKINGS_FILE_ZIP} could not be downloaded successfully, check log for more details"
	exit 1;
fi

# -------------------------------------------------------
# -- unzip the file, -c switch means the gz file is left intact
gunzip -c ${LOCAL_PATH}/${BOOKINGS_FILE_ZIP} > ${LOCAL_PATH}/${BOOKINGS_FILE_TXT}

# -------------------------------------------------------
# create target directory and upload file into HDFS
hdfs dfs -mkdir -p ${HDFS_PATH}
hdfs dfs -copyFromLocal ${LOCAL_PATH}/bookings_hits_${FILE_DATE} ${HDFS_PATH}

# -------------------------------------------------------
# -- create and upload success indicator into HDFS
touch ${LOCAL_PATH}/_SUCCESS
hdfs dfs -copyFromLocal ${LOCAL_PATH}/_SUCCESS ${HDFS_PATH}
