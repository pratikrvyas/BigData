#!/bin/bash

# -----------------------------------------------
#
#       Developer:         Satish Reddy (S751237)
#       Purpose:          iata_ftp_Script identifies the incremental files and copies them to HDFS
#
# -----------------------------------------------




HDFS_TARGET_DIR_AIRPORT_MASTER=$1
CONTROL_FILE=$2
LOCAL_DIR=$3
WRK_DIR=$4
HDFS_TARGET_DIR_AIRLINE_DESIGNATORS=$5
HDFS_TARGET_DIR_AIRCRAFT_TYPE=$6
HDFS_TARGET_DIR_UTC=$7
FTP_DETAILS=$8

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`

#HDFS_TARGET_DIR='/data/helix/temp/raw/iata/airport_master/incremental/1'
#CONTROL_FILE='/apps/helix/scripts/iata/iata_control.txt'
#LOCAL_DIR='/mnt/helix/ingest/iata/files'
#WRK_DIR='/mnt/helix/ingest/iata/work'


execution_date=`date +"%y%m%d"`
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
       New_files_not_available="IATA| FTP connection failed"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"

        exit 0;
fi

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d'  > $WRK_DIR/ftp_files_incremental.txt



if [ -s $WRK_DIR/ftp_files_incremental.txt ]
then
        echo "New files available"
else
       New_files_not_available="IATA| Files are not available. Please check!"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0;
fi

File_list=`cat $WRK_DIR/ftp_files_incremental.txt | xargs`
FTP_File_count=`cat $WRK_DIR/ftp_files_incremental.txt | wc -l`


while read -r line
do
  if [ $line = iata_airport_master.txt ] || [ $line = iata_airline_designators.txt ] || [ $line = iata_aircraft_types.txt ] || [ $line = iata_utc_local_time_comparisions.txt  ] ;
        then
            echo "file naming convention is correct"
			
			else 
			FILE_NAME="IATA | File name convention is incorrect.Please check! "
					  echo "BODY1=Actual Filename:$line ; Expected Filename:iata_airport_master.txt\iata_airline_designators.txt\iata_aircraft_types.txt\iata_utc_local_time_comparisions.txt"
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
      FTP_mismatch="IATA| FTP of few files is not successfull. Please check!"
          echo "ERRORVAL=FALSE"
           echo Mail_MSG=$FTP_mismatch
		   echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
		   exit 0;
      #`echo "Please check!"| mail -s "MIDT | FTP of few files is not successfull" satish.sathambakam@dnata.com`
 fi


#############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_TARGET_DIR_AIRLINE_DESIGNATORS}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_AIRLINE_DESIGNATORS}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_AIRCRAFT_TYPE}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_AIRCRAFT_TYPE}
        else
     echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_UTC}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_UTC}

else
 echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_AIRPORT_MASTER}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_DIR_AIRPORT_MASTER}

else
    echo "do nothing"
fi


 
 
############################################
#Creating directory and moving files to HDFS
############################################

hadoop fs -test -d ${HDFS_TARGET_DIR_AIRPORT_MASTER}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_AIRPORT_MASTER}
fi

hadoop fs -test -d ${HDFS_TARGET_DIR_AIRLINE_DESIGNATORS}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_AIRLINE_DESIGNATORS}
fi
hadoop fs -test -d ${HDFS_TARGET_DIR_AIRCRAFT_TYPE}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_AIRCRAFT_TYPE}
fi


hadoop fs -test -d ${HDFS_TARGET_DIR_UTC}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_DIR_UTC}
fi

         cat $WRK_DIR/ftp_files_incremental.txt  | while read line;
do


             if [[ $line = iata_airport_master* ]];
        then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_AIRPORT_MASTER}

            else 
			if [[ $line = iata_airline_designators* ]];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_AIRLINE_DESIGNATORS}
			  else 
			if [[ $line = iata_aircraft_types* ]];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_AIRCRAFT_TYPE}
			 else 
			if [[ $line = iata_utc_local_time_comparisions* ]];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_DIR_UTC}
			 else
			 echo "do nothing"
			 fi
			 fi 
			 fi
			 fi

done

execution_date=`date +"%y%m%d"`
echo "$execution_date;SUCCESS" | hdfs dfs -appendToFile - ${CONTROL_FILE}
	

 hdfs dfs -touchz ${HDFS_TARGET_DIR_AIRPORT_MASTER}/_SUCCESS
 hdfs dfs -touchz ${HDFS_TARGET_DIR_AIRLINE_DESIGNATORS}/_SUCCESS
  hdfs dfs -touchz ${HDFS_TARGET_DIR_AIRCRAFT_TYPE}/_SUCCESS
hdfs dfs -touchz ${HDFS_TARGET_DIR_UTC}/_SUCCESS

exit 0
