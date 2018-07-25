#!/bin/bash

# -----------------------------------------------
#
#       Developer:        Mohan S (s751093), spikej (s779208)
#       Purpose:          static_master_ftp_Script identifies the files and copies them to HDFS
#
# -----------------------------------------------

HDFS_TGT_DIR=$1
CONTROL_FILE=$2
LOCAL_DIR=$3
WRK_DIR=$4
FTP_DETAILS=$5
FILENAME=$6
execution_date=`date +"%y%m%d"`

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`

cd $WRK_DIR

rm -f $WRK_DIR/ftp_files_incremental.txt $WRK_DIR/ftp_log $WRK_DIR/wrk_pre_ftp_output_file_1.txt $WRK_DIR/file_found.txt
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
       New_files_not_available="MANUAL STATIC MASTER| FTP connection failed"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"

        exit 0;
fi

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }'  | sed '$d' | tr 'A-Z' 'a-z' > $WRK_DIR/ftp_files_incremental.txt



if [ -s $WRK_DIR/ftp_files_incremental.txt ]
then
        echo "New files available"
else
       New_files_not_available="MANUAL STATIC MASTER| Files are not available. Please check!"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0;
fi

File_list=`cat $WRK_DIR/ftp_files_incremental.txt | xargs`
FTP_File_count=`cat $WRK_DIR/ftp_files_incremental.txt | wc -l`

#tr 'A-Z' 'a-z' < ftp_files_incremental.txt

while read -r line
do
  if [ $line = $FILENAME ] ;
        then
            echo "$line found."
            echo "$line" >> $WRK_DIR/file_found.txt
                        else
            echo "$line found."
  fi

done < $WRK_DIR/ftp_files_incremental.txt

if [ -s $WRK_DIR/file_found.txt ] ;
then
	echo "File found. Proceeding to ingestion."
else
	FILE_NAME="MANUAL STATIC MASTER | Did not find correct file to ingest. Please check! "
	echo "BODY1=No file matching naming convention found in FTP. Expected Filename:$FILENAME "
	echo Mail_MSG=$FILE_NAME
	echo "ERRORVAL=FALSE"
	exit 0;
fi
				   
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


if grep -q "Not logged in" $WRK_DIR/ftp_log;
   then
   FTP_ISSUE="MANUAL STATIC MASTER| FTP connection failed"
            echo Mail_MSG=$FTP_ISSUE
                echo "ERRORVAL=FALSE"
                                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
   exit 0;
   else
   echo "no error"
   fi

Local_file_count=`ls -ltr * | wc -l`

echo "$Local_file_count"
echo "$FTP_File_count"
#comparing number of files in ftp server and local
##################################################

if [ "$Local_file_count" -ne "$FTP_File_count" ]
then
      FTP_mismatch="MANUAL STATIC MASTER| FTP of few files is not successful. Please check!"
          echo "ERRORVAL=FALSE"
           echo Mail_MSG=$FTP_mismatch
                   echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                   exit 0;

 fi

#############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_TGT_DIR}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TGT_DIR}

else
    echo "do nothing"
fi

############################################
#Creating directory and moving files to HDFS
############################################

hadoop fs -test -d ${HDFS_TGT_DIR}
if [ $? == 0 ]; then
    echo "exists"

else
    hdfs dfs -mkdir -p ${HDFS_TGT_DIR}
fi

#for i in $( ls | grep [A-Z] ); do mv -i $i `echo $i | tr 'A-Z' 'a-z'`; done

ls -ltr * | awk '{ print $9 }' | sed '/.zip/d'  | while read line;
do
        if [ $line = $FILENAME ] ;
        then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TGT_DIR}

            else
              echo "do nothing"
						 fi

done

execution_date=`date +"%y%m%d"`
echo "$execution_date;SUCCESS" | hdfs dfs -appendToFile - ${CONTROL_FILE}

hdfs dfs -touchz ${HDFS_TGT_DIR}/_SUCCESS

exit 0


