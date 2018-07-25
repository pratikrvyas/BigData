#!/bin/bash

# -----------------------------------------------
#
#       Developer:         Mohan Selvamani (S751093)
#       Purpose:          city_masters_ftp_Script identifies the incremental files and copies them to HDFS
#
# -----------------------------------------------

HDFS_TARGET_DIR_CITY_MASTER=$1
CONTROL_FILE=$2
LOCAL_DIR=$3
WRK_DIR=$4
FTP_DETAILS=$5

export HADOOP_USER_NAME=scv_ops

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`



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
       New_files_not_available="OAG CITY MASTER| FTP connection failed"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"

        exit 0;
fi

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d' > $WRK_DIR/ftp_files_incremental.txt



if [ -s $WRK_DIR/ftp_files_incremental.txt ]
then
        echo "New files available"
else
       New_files_not_available="OAG AIRPORT CITY MASTER | Files are not available. Please check!"
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
      FTP_mismatch="OAG AIRPORT CITY MASTER | FTP of few files is not successfull. Please check!"
          echo "ERRORVAL=FALSE"
           echo Mail_MSG=$FTP_mismatch
		   echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
		   exit 0;
      
 fi

 #############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_TARGET_DIR_CITY_MASTER}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_CITY_MASTER}

else
    echo "do nothing"
fi
############################################
#Creating directory and moving files to HDFS
############################################

hadoop fs -test -d ${HDFS_TARGET_DIR_CITY_MASTER}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_CITY_MASTER}
fi


 ls -ltr * | awk '{ print $9 }' | sed '/.zip/d' | while read line;
         do

           if [[ $line = Emirates* ]];
        then
           gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_CITY_MASTER}
			  			 else
			 echo "do nothing"
			 fi

done

echo "$execution_date;SUCCESS" | hdfs dfs -appendToFile - ${CONTROL_FILE}
	

 hdfs dfs -touchz ${HDFS_TARGET_DIR_CITY_MASTER}/_SUCCESS


exit 0
