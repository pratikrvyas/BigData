#!/bin/bash

# -----------------------------------------------
#
#       Developer:        Satish Reddy (S751237)
#       Purpose:          power_week_ftp_Script identifies the incremental files and copies them to HDFS
#
# -----------------------------------------------

HDFS_TARGET_DIR_CALENDER_DIM=$1
CONTROL_FILE=$2
LOCAL_DIR=$3
WRK_DIR=$4
HDFS_TARGET_DIR_POWER=$5

FTP_DETAILS=$6

export HADOOP_USER_NAME=scv_ops

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`


#HDFS_TARGET_DIR='/data/helix/temp/raw/iata/airport_master/incremental/1'
#CONTROL_FILE='/apps/helix/scripts/iata/iata_control.txt'
#LOCAL_DIR='/data/1/midt/iata'
#WRK_DIR='/data/1/helix/apps/scripts/iata'

##LOCAL_DIR='/data/1/temp/oag'
#FTP_HOST=ftp1.emirates.com
#FTP_USER=insight
#FTP_PASSWORD=!ns!ght
#FTP_DIR=/incoming/insight/POWERWEEK/
#WRK_DIR='/apps/helix/scripts/oag'
#HDFS_PATH='/data/helix/temp/raw/midt/midtpnr/incremental'

cd $WRK_DIR 

execution_date=`date +"%y%m%d"`
rm -f $WRK_DIR/ftp_files_incremental.txt $WRK_DIR/ftp_log $WRK_DIR/wrk_pre_ftp_output_file_1.txt
#importing file list from FTP SERVER
######################################
sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WRK_DIR/wrk_pre_ftp_output_file_1.txt 
cd $FTP_DIR
ls -l
quit
END_SCRIPT

if [ -s $WRK_DIR/wrk_pre_ftp_output_file_1.txt ]
then
        echo "FTP connection successful"
else
       New_files_not_available="POWER WEEK| FTP connection failed"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"

        exit 0;
fi

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d' > $WRK_DIR/ftp_files_incremental.txt


if [ -s $WRK_DIR/ftp_files_incremental.txt ]
then
        echo "New files available"
else
       New_files_not_available="POWER WEEK | Files are not available. Please check!"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0;
fi

File_list=`cat $WRK_DIR/ftp_files_incremental.txt | xargs`
FTP_File_count=`cat $WRK_DIR/ftp_files_incremental.txt | wc -l`




cd $LOCAL_DIR

rm -f $LOCAL_DIR/*

#importing new files to local directory
######################################
cat $WRK_DIR/ftp_files_incremental.txt | while read line;
do
sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WRK_DIR/ftp_log 
cd $FTP_DIR
mget $line
quit
END_SCRIPT

done

Local_file_count=`ls -ltr * | wc -l`

#comparing number of files in ftp server and local
##################################################

if [ "$Local_file_count" -ne "$FTP_File_count" ]
then
      FTP_mismatch="POWER WEEK| FTP of few files is not successfull. Please check!"
          echo "ERRORVAL=FALSE"
           echo Mail_MSG=$FTP_mismatch
		   echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
		   exit 0;
      #`echo "Please check!"| mail -s "MIDT | FTP of few files is not successfull" satish.sathambakam@dnata.com`
 fi

 
#############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_TARGET_DIR_CALENDER_DIM}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_CALENDER_DIM}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_POWER}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_POWER}

else
    echo "do nothing"
fi
############################################
#Creating directory and moving files to HDFS
############################################

hadoop fs -test -d ${HDFS_TARGET_DIR_CALENDER_DIM}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_CALENDER_DIM}
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_POWER}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_POWER}
fi

         cat $WRK_DIR/ftp_files_incremental.txt  | while read line;
do


             if [[ $line = calendar* ]];
        then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_CALENDER_DIM}

            else 
			if [[ $line = power* ]];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_POWER}
			 			 else
			 echo "do nothing"
			 fi
			 fi 
			

done

echo "$execution_date;SUCCESS" | hdfs dfs -appendToFile - ${CONTROL_FILE}
	

 hdfs dfs -touchz ${HDFS_TARGET_DIR_CALENDER_DIM}/_SUCCESS
 hdfs dfs -touchz ${HDFS_TARGET_DIR_POWER}/_SUCCESS

exit 0
