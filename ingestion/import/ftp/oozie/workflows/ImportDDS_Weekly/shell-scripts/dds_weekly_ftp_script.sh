# --------------------------------------------------------------------------------------------------------------------- #
#                                                                                                                       #
#       Developer:        vishnupriyap/AliAlHaddad/spikej (S454151, S403277, S779208)                                   #
#       Purpose:          dds_ftp_script identifies the incremental files, validates them, and copies them to HDFS      #
#                                                                                                                       #
# --------------------------------------------------------------------------------------------------------------------- #

# export HADOOP_USER_NAME=scv_ops

HDFS_PATH_DDS_02=$1
HDFS_PATH_DDS_03=$2
CONTROL_FILE_02=$3
CONTROL_FILE_03=$4
LOCAL_DIR=$5
WRK_DIR=$6
ERROR_LOG_FILE=$7
FTP_DETAILS=$8

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`

rm -f $WRK_DIR/debug.txt 


##############################################
# Find last processed date from control file #
##############################################

LAST_PROCESSED_DAY_02=`hdfs dfs -cat ${CONTROL_FILE_02}| tail -1| cut -d";" -f1` 
LAST_PROCESSED_DAY_03=`hdfs dfs -cat ${CONTROL_FILE_03}| tail -1| cut -d";" -f1` 

echo $(date --date "-`date +%d -d "$START_DATE"` days +2 month $START_DATE" +%Y-%m-%d)  

cd $WRK_DIR
rm -f $WRK_DIR/*.*


####################################
# Import file list from FTP Source #
####################################

sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WRK_DIR/wrk_pre_ftp_output_file_1.txt
cd $FTP_DIR
ls -l
quit
END_SCRIPT

if [ -s $WRK_DIR/wrk_pre_ftp_output_file_1.txt ]
then
  echo "FTP connection successful" >> $WRK_DIR/debug.txt
else
  New_files_not_available="DDS Ingestion | FTP connection failed"
  echo Mail_MSG=$New_files_not_available
  echo "ERRORVAL=FALSE"
  exit 0;
fi

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d' > $WRK_DIR/wrk_full_ftp_file_list.txt


#############################################################
# Check and populate new files to ftp_files_incremental.txt #
#############################################################

hdfs dfs -copyToLocal ${CONTROL_FILE_02} $WRK_DIR
hdfs dfs -copyToLocal ${CONTROL_FILE_03} $WRK_DIR
while read line;
do
  var0=${line:0:20}
  if [ $var0 == "2017011911403_weekly" ]
  then
    echo "$line is a weekly 03 file" >> $WRK_DIR/debug.txt
    echo "$line" >> $WRK_DIR/weekly_03_file_list.txt
    var1=${line:21:10}
    echo "$var1" >> $WRK_DIR/03_date_list.txt
  elif [ $var0 == "2017011911402_weekly" ]
  then
    echo "$line is a weekly 02 file" >> $WRK_DIR/debug.txt
    echo "$line" >> $WRK_DIR/weekly_02_file_list.txt
    var1=${line:21:10}
    echo "$var1" >> $WRK_DIR/02_date_list.txt
  else
    echo "$line is not a weekly file" >> $WRK_DIR/debug.txt
  fi
  job_rc=$?
  if [[ $job_rc -ne 0 ]];
    then
    exit 2;
  else
    echo "no error'"
  fi
done < $WRK_DIR/wrk_full_ftp_file_list.txt

sort $WRK_DIR/03_date_list.txt -o $WRK_DIR/03_date_list.txt
sort $WRK_DIR/02_date_list.txt -o $WRK_DIR/02_date_list.txt

while read line;
do
  LINE_DATE=$(date --date "$line" +%Y%m%d) 
  LPD=$(date --date "$LAST_PROCESSED_DAY_03" +%Y%m%d) 
  if [ $LINE_DATE -gt $LPD ]
  then
    echo "Earliest 03-file data after $LAST_PROCESSED_DAY_03 available from $line. Ingesting file..." >> $WRK_DIR/debug.txt
    execution_date=`date +"%Y-%m-%d %T"`
    echo "$line;$execution_date" >> $WRK_DIR/control_file_03_update.txt
    grep "$line" weekly_03_file_list.txt >> $WRK_DIR/03_file_to_download.txt
    grep "$line" weekly_03_file_list.txt >> $WRK_DIR/files_to_download.txt
    break
  else
    echo "$line filename indicates it does not contain data later than $LAST_PROCESSED_DAY_03. File will not be ingested." >> $WRK_DIR/debug.txt
  fi
  job_rc=$?
  if [[ $job_rc -ne 0 ]];
    then
    exit 2;
  else
    echo "No error" >> $WRK_DIR/debug.txt
  fi
done < $WRK_DIR/03_date_list.txt

while read line;
do
  LINE_DATE=$(date --date "$line" +%Y%m%d) 
  LPD=$(date --date "$LAST_PROCESSED_DAY_02" +%Y%m%d) 
  if [ $LINE_DATE -gt $LPD ]
  then
    echo "Earliest 02-file data after $LAST_PROCESSED_DAY_02 available from $line. Ingesting file..." >> $WRK_DIR/debug.txt
    execution_date=`date +"%Y-%m-%d %T"`
    echo "$line;$execution_date" >> $WRK_DIR/control_file_02_update.txt
    grep "$line" weekly_02_file_list.txt >> $WRK_DIR/02_file_to_download.txt
    grep "$line" weekly_02_file_list.txt >> $WRK_DIR/files_to_download.txt
    break
  else
    echo "$line filename indicates it does not contain data later than $LAST_PROCESSED_DAY_02. File will not be ingested." >> $WRK_DIR/debug.txt
  fi
  job_rc=$?
  if [[ $job_rc -ne 0 ]];
    then
    exit 2;
  else
    echo "No error" >> $WRK_DIR/debug.txt
  fi
done < $WRK_DIR/02_date_list.txt

rm -f $WRK_DIR/dds_02_control.txt
rm -f $WRK_DIR/dds_03_control.txt

chmod 777 $WRK_DIR/*

if [ -s $WRK_DIR/files_to_download.txt ]
then
  echo "New files available" >> $WRK_DIR/debug.txt
else
  New_files_not_available="DDS Ingestion | No new DDS files are available. Please check!"
  echo Mail_MSG=$New_files_not_available
  echo "ERRORVAL=FALSE"
  exit 0;
fi

File_list=`cat files_to_download.txt | xargs`
echo FTP_FILES=$File_list
cd $LOCAL_DIR
rm -f $LOCAL_DIR/*


#######################################
# Import new files to local directory #
#######################################

cat $WRK_DIR/files_to_download.txt | while read line;
do
sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WRK_DIR/ftp_log
cd $FTP_DIR
lcd $LOCAL_DIR
mget $line
quit
END_SCRIPT
done


#########################################
# Delete/create target HDFS directories #
#########################################

cd $LOCAL_DIR

hadoop fs -test -d ${HDFS_PATH_DDS_03}
if [ $? == 0 ]; then
  echo "exists" >> $WRK_DIR/debug.txt
  hdfs dfs -rm -r ${HDFS_PATH_DDS_03}
else
  echo "do nothing" >> $WRK_DIR/debug.txt
fi
  hdfs dfs -mkdir -p ${HDFS_PATH_DDS_03}

hadoop fs -test -d ${HDFS_PATH_DDS_02}
if [ $? == 0 ]; then
  echo "exists" >> $WRK_DIR/debug.txt
  hdfs dfs -rm -r ${HDFS_PATH_DDS_02}
else
  echo "do nothing" >> $WRK_DIR/debug.txt
fi
  hdfs dfs -mkdir -p ${HDFS_PATH_DDS_02}


###################################################################
# Move files to HDFS,and create success flag for BOTH directories #
###################################################################

if [ -s $WRK_DIR/03_file_to_download.txt ]
then
  cat $WRK_DIR/03_file_to_download.txt | sed '/.zip/d'  | while read line;
  do
    hadoop fs -test -d ${HDFS_PATH_DDS_02}
    if [ $? == 0 ]; then
      echo "$line" >> $WRK_DIR/debug.txt
      echo "${LOCAL_DIR}/$line ${HDFS_PATH_DDS_03}" >> $WRK_DIR/debug.txt
      hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line ${HDFS_PATH_DDS_03}
      job_rc=$?
      if [[ $job_rc -ne 0 ]];
      then
        HDFS_03_copy_error="DDS Ingestion | Could not copy file to HDFS"
        echo Mail_MSG=$HDFS_03_copy_error
        HDFS_03_copy_error_body="Could not copy 03 file to HDFS. Please check directories and user permissions."$'\n\n'"Local Path: ${LOCAL_DIR}/$line"$'\n'"HDFS Directory: {HDFS_PATH_DDS_03}"
        echo BODY1=$HDFS_03_copy_error_body
        echo "ERRORVAL=FALSE"
        exit 0;
      else 
        echo "No error; exiting job" >> $WRK_DIR/debug.txt
      fi
    else
      HDFS_03_not_available="DDS Ingestion | HDFS Directory Unavailable"
      echo Mail_MSG=$New_files_not_available
      HDFS_03_not_available_body="The HDFS target directory is not available. Please check user permissions."$'\n\n'"Target Directory: (${HDFS_PATH_DDS_03})"
      echo BODY1=$HDFS_03_not_available_body
      echo "ERRORVAL=FALSE"
      exit 0;
    fi
  done
else
    echo "do nothing" >> $WRK_DIR/debug.txt
fi

hdfs dfs -touchz ${HDFS_PATH_DDS_03}/_SUCCESS

if [ -s $WRK_DIR/02_file_to_download.txt ]
then
  cat $WRK_DIR/02_file_to_download.txt | sed '/.zip/d'  | while read line;
  do
    hadoop fs -test -d ${HDFS_PATH_DDS_02}
    if [ $? == 0 ]; then
      echo "$line" >> $WRK_DIR/debug.txt
      echo "${LOCAL_DIR}/$line ${HDFS_PATH_DDS_02}" >> $WRK_DIR/debug.txt
      hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line ${HDFS_PATH_DDS_02}
      job_rc=$?
      if [[ $job_rc -ne 0 ]];
      then
        HDFS_02_copy_error="DDS Ingestion | Could not copy file to HDFS"
        echo Mail_MSG=$HDFS_02_copy_error
        HDFS_02_copy_error_body="Could not copy 02 file to HDFS. Please check directories and user permissions."$'\n\n'"Local Path: ${LOCAL_DIR}/$line"$'\n'"HDFS Directory: {HDFS_PATH_DDS_02}"
        echo BODY1=$HDFS_02_copy_error_body
        echo "ERRORVAL=FALSE"
        exit 0;
      else 
        echo "No error; exiting job" >> $WRK_DIR/debug.txt
      fi
    else
      HDFS_02_not_available="DDS Ingestion | HDFS Directory Unavailable"
      echo Mail_MSG=$New_files_not_available
      HDFS_02_not_available_body="The HDFS target directory is not available. Please check user permissions."$'\n\n'"Target Directory: (${HDFS_PATH_DDS_02})"
      echo BODY1=$HDFS_02_not_available_body
      echo "ERRORVAL=FALSE"
      exit 0;
    fi
  done
else
    echo "do nothing" >> $WRK_DIR/debug.txt
fi

hdfs dfs -touchz ${HDFS_PATH_DDS_02}/_SUCCESS


#####################
#Update control file#
#####################

cat $WRK_DIR/control_file_03_update.txt | hdfs dfs -appendToFile - ${CONTROL_FILE_03}
cat $WRK_DIR/control_file_02_update.txt | hdfs dfs -appendToFile - ${CONTROL_FILE_02}

job_rc=$?
if [[ $job_rc -ne 0 ]];
then
  exit 6;
else 
  echo "No error; exiting job" >> $WRK_DIR/debug.txt
fi

exit 0
