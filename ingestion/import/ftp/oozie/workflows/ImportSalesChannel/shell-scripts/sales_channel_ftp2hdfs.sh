# ----------------------------------------------------------------
#
#       Developer:        Mohan Selvamani/Satish Reddy/vishnupriyap/spikej (S751093, S751237, S454151, S779208)
#       Purpose:          accelya_ftp_script identifies the incremental files, validates them, and copies them to HDFS
#
# ------------------------------------------------------------------

T_D_SALES_CHANNEL=$1
CONTROL_FILE=$2
LOCAL_DIR=$3
WORK_DIR=$4
FTP_DETAILS=$5
TIMESTAMP=$6

export HADOOP_USER_NAME=scv_ops

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`

###################################
# Delete previous temporary files #
###################################

cd $WORK_DIR
chmod 777 *.*
rm -f $WORK_DIR/*.*
rm -f $LOCAL_DIR/*.*


####################################
# Import file list from FTP Source #
####################################

sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WORK_DIR/wrk_pre_ftp_output_file_1.txt
cd $FTP_DIR
ls -l
quit
END_SCRIPT

if [ -s $WORK_DIR/wrk_pre_ftp_output_file_1.txt ]
then
  echo "FTP connection successful" >> $WORK_DIR/debug.txt
else
  MAIL_SUBJECT="Sales Channel Ingestion | FTP connection failed"
  echo "MAIL_SUBJECT=$MAIL_SUBJECT"
  MAIL_BODY="Could not make connection to FTP server. Please check server availability, firewall connection and FTP credentials."
  echo "MAIL_BODY=$MAIL_BODY"
  echo "ERRORVAL=TRUE"
  exit 0;
fi

cat $WORK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d' > $WORK_DIR/wrk_full_ftp_file_list.txt


###########################################################
# Check and populate new files to control_file_update.txt #
###########################################################

if [ -s $WORK_DIR/wrk_full_ftp_file_list.txt ]
then
  echo "New files available" >> $WORK_DIR/debug.txt
else
  MAIL_SUBJECT="Sales Channel Ingestion | No files are available"
  echo "MAIL_SUBJECT=$MAIL_SUBJECT"
  MAIL_BODY="Operation did not find any files in the source FTP directory. Please check file presence and visibility for configured user account."
  echo "MAIL_BODY=$MAIL_BODY"
  echo "ERRORVAL=TRUE"
  exit 0;
fi
SYSDATETIME=$(date +%Y/%m/%d_%H:%M:%S.%N)
while read line;
do
  echo "$line found" >> $WORK_DIR/debug.txt
  echo "$TIMESTAMP;$line;$SYSDATETIME" >> $WORK_DIR/control_file_update.txt
done < $WORK_DIR/wrk_full_ftp_file_list.txt


#######################################
# Import new files to local directory #
#######################################

while read line;
do
sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WORK_DIR/ftp_log
cd $FTP_DIR
lcd $LOCAL_DIR
mget $line
quit
END_SCRIPT
done < $WORK_DIR/wrk_full_ftp_file_list.txt


############################################
# Check HDFS directory; delete if existing #
############################################

cd $LOCAL_DIR
chmod 777 *.*
hadoop fs -test -d ${T_D_SALES_CHANNEL}
if [ $? == 0 ]; then
  echo "Directory exists: $T_D_SALES_CHANNEL" >> $WORK_DIR/debug.txt
  hdfs dfs -rm -r ${T_D_SALES_CHANNEL}
else
  echo "Directory does not exist: $T_D_SALES_CHANNEL" >> $WORK_DIR/debug.txt
fi

#########################
# Create HDFS directory #
#########################

hdfs dfs -mkdir -p ${T_D_SALES_CHANNEL}


##########################
# Upload file(s) to HDFS #
##########################

while read line;
do
  hadoop fs -test -d ${T_D_SALES_CHANNEL}
  if [ $? == 0 ]; then
    echo "Uploading ${LOCAL_DIR}/$line to ${T_D_SALES_CHANNEL}..." >> $WORK_DIR/debug.txt
    hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line ${T_D_SALES_CHANNEL}
    job_rc=$?
    if [[ $job_rc -ne 0 ]];
    then
      MAIL_SUBJECT="Sales Channel Ingestion | Could not copy file to HDFS"
      echo MAIL_SUBJECT=$MAIL_SUBJECT
      MAIL_BODY="Could not copy file to HDFS. Please check directories and user permissions."$'\n\n'"Local Path: ${LOCAL_DIR}/$line"$'\n'"HDFS Directory: ${T_D_SALES_CHANNEL}"
      echo MAIL_BODY=$MAIL_BODY
      echo "ERRORVAL=TRUE"
      exit 0;
    else 
      echo "Upload complete" >> $WORK_DIR/debug.txt
    fi
  else
    MAIL_SUBJECT="Sales Channel Ingestion | HDFS Directory Unavailable"
    echo MAIL_SUBJECT=$MAIL_SUBJECT
    MAIL_BODY="The HDFS target directory is not available after attempted file upload. Please check user permissions."$'\n\n'"Target Directory: ${T_D_SALES_CHANNEL}"
    echo MAIL_BODY=$MAIL_BODY
    echo "ERRORVAL=TRUE"
    exit 0;
  fi
done < $WORK_DIR/wrk_full_ftp_file_list.txt


#######################
# Create success flag #
#######################

hdfs dfs -touchz ${T_D_SALES_CHANNEL}/_SUCCESS


#######################
# Update control file #
#######################

hadoop fs -test -f ${CONTROL_FILE}

job_rc=$?
if [[ $job_rc -ne 0 ]];
then
  MAIL_SUBJECT="Sales Channel Ingestion | Could not update control file"
  echo MAIL_SUBJECT=$MAIL_SUBJECT
  MAIL_BODY="Could not copy file to HDFS. Please check directories and user permissions."$'\n\n'"Control File Path: ${CONTROL_FILE}"$'\n'"Pending Update: ${CONTROL_FILE_UPDATE}"
  echo MAIL_BODY=$MAIL_BODY
  echo "ERRORVAL=TRUE"
  exit 0;
else 
  echo "Control file found: ${CONTROL_FILE}" >> $WORK_DIR/debug.txt
fi

CONTROL_FILE_UPDATE=$(cat $WORK_DIR/control_file_update.txt)

cat $WORK_DIR/control_file_update.txt | hdfs dfs -appendToFile - ${CONTROL_FILE}

job_rc=$?
if [[ $job_rc -ne 0 ]];
then
  MAIL_SUBJECT="Sales Channel Ingestion | Could not update control file"
  echo MAIL_SUBJECT=$MAIL_SUBJECT
  MAIL_BODY="Could not copy file to HDFS. Please check directories and user permissions."$'\n\n'"Control File Path: ${CONTROL_FILE}"$'\n'"Pending Update: ${CONTROL_FILE_UPDATE}"
  echo MAIL_BODY=$MAIL_BODY
  echo "ERRORVAL=TRUE"
  exit 0;
else 
  echo "Control file upated" >> $WORK_DIR/debug.txt
fi

exit 0
