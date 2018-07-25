#!/bin/bash

# -----------------------------------------------
#
#       Developer:         Satish Reddy (S751237)
#       Purpose:          network_hierarchy_ftp_Script identifies the incremental files and copies them to HDFS
#
# -----------------------------------------------

HDFS_TARGET_AREA_REGION=$1
HDFS_TARGET_COUNTRY_CURRENCY=$2
HDFS_TARGET_TERRITORY=$3
CONTROL_FILE=$4
LOCAL_DIR=$5
WRK_DIR=$6
FTP_DETAILS=$7
HDFS_TARGET_CITY=$8
HDFS_TARGET_FINANCIALYEAR_SUMMARY=$9

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`

export HADOOP_USER_NAME=scv_ops

#HDFS_TARGET_TERRITORY_MASTER='/data/helix/raw/citymaster/CITY_MASTER/incremental/2016-09-06/09-16'
#CONTROL_FILE='/apps/helix/scripts/iata/iata_control.txt'
#LOCAL_DIR='/data/1/iata'
#WRK_DIR='/data/1/helix/apps/scripts/citymaster'


#FTP_HOST=ftp1.emirates.com
#FTP_USER=insight
#FTP_PASSWORD=!ns!ght
#FTP_DIR=/incoming/insight/CITY_MASTER/


cd $WRK_DIR 


execution_date=`date +"%y%m%d"`
rm -f $WRK_DIR/ftp_files_incremental.txt $WRK_DIR/ftp_log $WRK_DIR/wrk_pre_ftp_output_file_1.txt 
#importing file list from FTP SERVER
######################################

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
       New_files_not_available="Network Hierarchy  | FTP connection failed"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"

        exit 0;
fi

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d' > $WRK_DIR/ftp_files_incremental.txt



if [ -s $WRK_DIR/ftp_files_incremental.txt ]
then
        echo "New files available"
else
       New_files_not_available="Network Hierarchy File Data ingestion – Failure Notification"
            echo Mail_MSG=$New_files_not_available
			echo "BODY1=FTP files does not exists in FTP server"
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0;
fi

File_list=`cat $WRK_DIR/ftp_files_incremental.txt | xargs`
FTP_File_count=`cat $WRK_DIR/ftp_files_incremental.txt | wc -l`



while read -r line
do
  if [ $line = country_territory.csv ] || [ $line = area_region.csv ] || [ $line = region_country_currency.csv ] || [ $line = city_country_territory.csv  ] || [ $line = financialyear_summary.csv  ]  ;
        then
            echo "file naming convention is correct"
			
			else 
			FILE_NAME="Network Hierarchy  | File name convention is incorrect.Please check! "
					  echo "BODY1=Actual Filename:$line ; Expected Filename:country_territory.csv\area_region.csv\region_country_currency.csv\city_country_territory.csv\financialyear_summary.csv "
				      echo Mail_MSG=$FILE_NAME
                      echo "ERRORVAL=FALSE"
				      exit 0;



  fi

done < $WRK_DIR/ftp_files_incremental.txt 

if grep -q country_territory $WRK_DIR/ftp_files_incremental.txt;
then
	  if grep -q area_region $WRK_DIR/ftp_files_incremental.txt;
               then
			     if grep -q region_country_currency $WRK_DIR/ftp_files_incremental.txt;
               then
			   if grep -q city_country_territory $WRK_DIR/ftp_files_incremental.txt;
               then
			   if grep -q financialyear_summary $WRK_DIR/ftp_files_incremental.txt;
               then
			 echo "all files exist"
			 else
             New_files_not_available="Network Hierarchy File Data ingestion – Failure Notification - Few files are missing"
            echo Mail_MSG=$New_files_not_available
			echo "BODY1=Few files are not available. Please check"
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0
				  fi
				   else
             New_files_not_available="Network Hierarchy File Data ingestion – Failure Notification - Few files are missing"
            echo Mail_MSG=$New_files_not_available
			echo "BODY1=Few files are not available. Please check"
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0
				  fi
				   else
             New_files_not_available="Network Hierarchy File Data ingestion – Failure Notification - Few files are missing"
            echo Mail_MSG=$New_files_not_available
			echo "BODY1=Few files are not available. Please check"
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0
				  fi
				   else
             New_files_not_available="Network Hierarchy File Data ingestion – Failure Notification - Few files are missing"
            echo Mail_MSG=$New_files_not_available
			echo "BODY1=Few files are not available. Please check"
                echo "ERRORVAL=FALSE"
                echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
                  exit 0
				  fi
				   else
             New_files_not_available="Network Hierarchy File Data ingestion – Failure Notification - Few files are missing"
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


Local_file_count=`ls -ltr * | wc -l`

#comparing number of files in ftp server and local
##################################################

if [ "$Local_file_count" -ne "$FTP_File_count" ]
then
      FTP_mismatch="NETWORK HIERARCHY| FTP of few files is not successfull. Please check!"
          echo "ERRORVAL=FALSE"
           echo Mail_MSG=$FTP_mismatch
		   echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
		   exit 0;
      #`echo "Please check!"| mail -s "MIDT | FTP of few files is not successfull" satish.sathambakam@dnata.com`
 fi
 #############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_TARGET_AREA_REGION}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_AREA_REGION}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_COUNTRY_CURRENCY}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_COUNTRY_CURRENCY}

else
    echo "do nothing"
fi


hadoop fs -test -d ${HDFS_TARGET_TERRITORY}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_TERRITORY}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_CITY}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_CITY}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_TARGET_FINANCIALYEAR_SUMMARY}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_TARGET_FINANCIALYEAR_SUMMARY}

else
    echo "do nothing"
fi
############################################
#Creating directory and moving files to HDFS
############################################

hadoop fs -test -d ${HDFS_TARGET_AREA_REGION}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_AREA_REGION}
fi

hadoop fs -test -d ${HDFS_TARGET_COUNTRY_CURRENCY}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_COUNTRY_CURRENCY}
fi

hadoop fs -test -d ${HDFS_TARGET_TERRITORY}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_TERRITORY}
fi

hadoop fs -test -d ${HDFS_TARGET_CITY}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_CITY}
fi

hadoop fs -test -d ${HDFS_TARGET_FINANCIALYEAR_SUMMARY}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_TARGET_FINANCIALYEAR_SUMMARY}
fi

 cat $WRK_DIR/ftp_files_incremental.txt  | while read line;
do


             if [[ $line = country_territory* ]];
        then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_TERRITORY}

            else 
			if [[ $line = area_region* ]];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_AREA_REGION}
			 			 else
						 if [[ $line = region_country_currency* ]];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_COUNTRY_CURRENCY}
			  else
						 if [[ $line = financialyear_summary* ]];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_FINANCIALYEAR_SUMMARY}
			 			 else
						 
						 if [[ $line = city_country_territory* ]];
			then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TARGET_CITY}
			 			 else
			 echo "do nothing"
			 fi
			 fi 
			 fi
			 fi
			 fi
			

done

echo "$execution_date;SUCCESS" | hdfs dfs -appendToFile - ${CONTROL_FILE}
	

 hdfs dfs -touchz ${HDFS_TARGET_TERRITORY}/_SUCCESS
 hdfs dfs -touchz ${HDFS_TARGET_AREA_REGION}/_SUCCESS
 hdfs dfs -touchz ${HDFS_TARGET_COUNTRY_CURRENCY}/_SUCCESS
 hdfs dfs -touchz ${HDFS_TARGET_CITY}/_SUCCESS
hdfs dfs -touchz ${HDFS_TARGET_FINANCIALYEAR_SUMMARY}/_SUCCESS

exit 0
