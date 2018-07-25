#!/bin/bash

# -----------------------------------------------
#HDFS_TARGET_DIR_AIRCRAFT_TYPE
#       Developer:        Mohan(s751093)
#       Purpose:          static_master_ftp_Script identifies the files and copies them to HDFS
#
# -----------------------------------------------




HDFS_TARGET_DIR_CDW_COM_AIRPORTS_OD=$1
HDFS_TARGET_DIR_CDW_DIST_RETRACTION_MASTER=$2
HDFS_TARGET_DIR_CDW_IATA_AREA_MASTER=$3
HDFS_TARGET_DIR_CDW_OD_EXCEPTIONS=$4
HDFS_TARGET_DIR_CDW_OD_PARAMETERS=$5

CONTROL_FILE=$6
LOCAL_DIR=$7
WRK_DIR=$8
FTP_DETAILS=$9
execution_date=`date +"%y%m%d"`

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`

export HADOOP_USER_NAME=scv_ops

#HDFS_TARGET_DIR='/data/helix/raw/Static/CDW_*/incremental/1';  
#CONTROL_FILE='/apps/helix/scripts/cdw_static_master/cdw_static_master_control.txt'
#LOCAL_DIR='/mnt/helix/ingest/cdw_static_master/files' incoming/insight/STATIC/OD_MASTERS
#WRK_DIR='/mnt/helix/ingest/cdw_static_master/work'



cd $WRK_DIR

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

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }'  | sed '$d' | tr 'A-Z' 'a-z' > $WRK_DIR/ftp_files_incremental.txt



if [ -s $WRK_DIR/ftp_files_incremental.txt ]
then
        echo "New files available"
else
       New_files_not_available="CDW STATIC MASTER| Files are not available. Please check!"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0;
fi

File_list=`cat $WRK_DIR/ftp_files_incremental.txt | xargs`
FTP_File_count=`cat $WRK_DIR/ftp_files_incremental.txt | wc -l`

while read -r line
do
  if [ $line = cdw_com_airports_od.csv ] || [ $line = cdw_dist_retraction_master.csv ] || [ $line = cdw_iata_area_master.csv ] || [ $line = cdw_od_exceptions.csv ] || [ $line = cdw_od_parameters.csv ] ;
        then
            echo "file naming convention is correct"

                        else
                        FILE_NAME="MANUAL SECTOR MASTER  | File name convention is incorrect.Please check! "
                                          echo "BODY1=Actual Filename:$line ; Expected Filename:cdw_com_airports_od.csv\cdw_dist_retraction_master.csv\cdw_iata_area_master.csv\cdw_od_exceptions.csv\cdw_od_parameters.csv "
                                      echo Mail_MSG=$FILE_NAME
                      echo "ERRORVAL=FALSE"
                                      exit 0;



  fi

done < $WRK_DIR/ftp_files_incremental.txt


if grep -q cdw_com_airports_od $WRK_DIR/ftp_files_incremental.txt;
then
	  if grep -q cdw_dist_retraction_master $WRK_DIR/ftp_files_incremental.txt;
               then
			     if grep -q cdw_iata_area_master $WRK_DIR/ftp_files_incremental.txt;
               then
			   if grep -q cdw_od_exceptions $WRK_DIR/ftp_files_incremental.txt;
               then
			   if grep -q cdw_od_parameters $WRK_DIR/ftp_files_incremental.txt;
               then
			 echo "all files exist"
			 else
             New_files_not_available="Static Master File Data ingestion – Failure Notification - Few files are missing"
            echo Mail_MSG=$New_files_not_available
			echo "BODY1=Few files are not available. Please check"
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0
				  fi
				   else
             New_files_not_available="Static Master File Data ingestion – Failure Notification - Few files are missing"
            echo Mail_MSG=$New_files_not_available
			echo "BODY1=Few files are not available. Please check"
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0
				  fi
				   else
             New_files_not_available="Static Master File Data ingestion – Failure Notification - Few files are missing"
            echo Mail_MSG=$New_files_not_available
			echo "BODY1=Few files are not available. Please check"
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0
				  fi
				   else
             New_files_not_available="Static Master File Data ingestion – Failure Notification - Few files are missing"
            echo Mail_MSG=$New_files_not_available
			echo "BODY1=Few files are not available. Please check"
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0
				  fi
				   else
             New_files_not_available="Static Master File Data ingestion – Failure Notification - Few files are missing"
            echo Mail_MSG=$New_files_not_available
			echo "BODY1=Few files are not available. Please check"
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0
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
   FTP_ISSUE="CDW STATIC MASTER| FTP connection failed"
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
      FTP_mismatch="CDW STATIC MASTER| FTP of few files is not successful. Please check!"
          echo "ERRORVAL=FALSE"
           echo Mail_MSG=$FTP_mismatch
		   echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
		   exit 0;
      #`echo "Please check!"| mail -s "MIDT | FTP of few files is not successful" kk.pradeep@emirates.com`
 fi
#############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_TARGET_DIR_CDW_COM_AIRPORTS_OD}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_CDW_COM_AIRPORTS_OD}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_CDW_DIST_RETRACTION_MASTER}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_CDW_DIST_RETRACTION_MASTER}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_CDW_IATA_AREA_MASTER}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_CDW_IATA_AREA_MASTER}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_CDW_OD_EXCEPTIONS}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_CDW_OD_EXCEPTIONS}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_CDW_OD_PARAMETERS}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_CDW_OD_PARAMETERS}

else
    echo "do nothing"
fi




############################################
#Creating directory and moving files to HDFS
############################################

hadoop fs -test -d ${HDFS_TARGET_DIR_CDW_COM_AIRPORTS_OD}
if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_CDW_COM_AIRPORTS_OD}
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_CDW_DIST_RETRACTION_MASTER}
if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_CDW_DIST_RETRACTION_MASTER}
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_CDW_IATA_AREA_MASTER}
if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_CDW_IATA_AREA_MASTER}
fi


hadoop fs -test -d ${HDFS_TARGET_DIR_CDW_OD_EXCEPTIONS}
if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_CDW_OD_EXCEPTIONS}
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_CDW_OD_PARAMETERS}
if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_CDW_OD_PARAMETERS}
fi

         cat $WRK_DIR/ftp_files_incremental.txt  | while read line;
do
        if [ $line = cdw_com_airports_od* ] || [ $line = CDW_COM_AIRPORTS_OD* ];
        then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_CDW_COM_AIRPORTS_OD}

            else 
			if [ $line = cdw_dist_retraction_master* ] || [ $line = CDW_DIST_RETRACTION_MASTER* ];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_CDW_DIST_RETRACTION_MASTER}
			  else 
			if [ $line = cdw_iata_area_master* ] || [ $line = CDW_IATA_AREA_MASTER* ];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_CDW_IATA_AREA_MASTER}
			 else 
			if [ $line = cdw_od_exceptions* ] || [ $line = CDW_OD_EXCEPTIONS* ];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_CDW_OD_EXCEPTIONS}
			 else
			 if [ $line = cdw_od_parameters* ] || [ $line = CDW_OD_PARAMETERS* ];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_CDW_OD_PARAMETERS}
			 else
			 echo "do nothing"
			 fi
			 fi 
			 fi
			 fi
             fi
done

execution_date=`date +"%y%m%d"`
echo "$execution_date;SUCCESS" | hdfs dfs -appendToFile - ${CONTROL_FILE}
	
hdfs dfs -touchz ${HDFS_TARGET_DIR_CDW_COM_AIRPORTS_OD}/_SUCCESS
hdfs dfs -touchz ${HDFS_TARGET_DIR_CDW_DIST_RETRACTION_MASTER}/_SUCCESS
hdfs dfs -touchz ${HDFS_TARGET_DIR_CDW_IATA_AREA_MASTER}/_SUCCESS
hdfs dfs -touchz ${HDFS_TARGET_DIR_CDW_OD_EXCEPTIONS}/_SUCCESS
hdfs dfs -touchz ${HDFS_TARGET_DIR_CDW_OD_PARAMETERS}/_SUCCESS

exit 0