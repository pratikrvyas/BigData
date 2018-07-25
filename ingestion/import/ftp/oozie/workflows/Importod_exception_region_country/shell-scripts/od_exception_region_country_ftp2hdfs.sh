#!/bin/bash

# -----------------------------------------------
#
#       Developer:         Ali Al Haddad (S403277)
#       Purpose:         OD_EXCEPTION_REGION_COUNTRY_Script identifies the incremental files and copies them to HDFS
#
# -----------------------------------------------
HDFS_TARGET_OD_EXCEPTION_REGION_COUNTRY=$1
CONTROL_FILE=$2
LOCAL_DIR=$3
WRK_DIR=$4
FTP_DETAILS=$5


FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`

export HADOOP_USER_NAME=scv_ops

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
       New_files_not_available="OD_EXCEPTION_REGION_COUNTRY | FTP connection failed"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"

        exit 0;
fi

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d' | tr 'A-Z' 'a-z' > $WRK_DIR/ftp_files_incremental.txt



if [ -s $WRK_DIR/ftp_files_incremental.txt ]
then
        echo "New files available"
else
       New_files_not_available="OD_EXCEPTION_REGION_COUNTRY | Files are not available. Please check!"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0;
fi

File_list=`cat $WRK_DIR/ftp_files_incremental.txt | xargs`
FTP_File_count=`cat $WRK_DIR/ftp_files_incremental.txt | wc -l`

while read -r line
do
  if [ $line = od_exception_region_country.csv ] ;
        then
            echo "file naming convention is correct"

                        else
                        FILE_NAME="od exception region country  | File name convention is incorrect.Please check! "
                                          echo "BODY1=Actual Filename:$line ; Expected Filename:od_exception_region_country.csv "
                                      echo Mail_MSG=$FILE_NAME
                      echo "ERRORVAL=FALSE"
                                      exit 0;

  fi

done < $WRK_DIR/ftp_files_incremental.txt



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
      FTP_mismatch="OD_EXCEPTION_REGION_COUNTRY | FTP of few files is not successfull. Please check!"
          echo "ERRORVAL=FALSE"
           echo Mail_MSG=$FTP_mismatch
                    echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                    exit 0;

 fi
#############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_TARGET_OD_EXCEPTION_REGION_COUNTRY}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_OD_EXCEPTION_REGION_COUNTRY}

else
    echo "do nothing"
fi

############################################
#Creating directory and moving files to HDFS
############################################

hadoop fs -test -d ${HDFS_TARGET_OD_EXCEPTION_REGION_COUNTRY}

if [ $? == 0 ]; then
    echo "exists"

else
    hdfs dfs -mkdir -p ${HDFS_TARGET_OD_EXCEPTION_REGION_COUNTRY}
fi


  ls -ltr * | awk '{ print $9 }' | sed '/.zip/d' | while read line;
do
        if [[ $line = od_exception_region_country* ]];
        then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_OD_EXCEPTION_REGION_COUNTRY}

            else
                          if [[ $line = OD_EXCEPTION_REGION_COUNTRY* ]];
                          then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_OD_EXCEPTION_REGION_COUNTRY}
                          else
                          echo "do nothing"
                          fi
                          fi

done

echo "$execution_date;SUCCESS" | hdfs dfs -appendToFile - ${CONTROL_FILE}


hdfs dfs -touchz ${HDFS_TARGET_OD_EXCEPTION_REGION_COUNTRY}/_SUCCESS


exit 0
