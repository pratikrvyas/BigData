#!/bin/bash

# --------------------------------------------------------------#
#                                                               #
#       Developer:        Ali Al Haddad(s403277), s779208       #
#       Purpose:          PRISM                                 #
#                                                               #
# --------------------------------------------------------------#

export HADOOP_USER_NAME=scv_ops

HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShare=$1
HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTracking=$2
HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShareControlFile=$3
HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTrackerControlFile=$4
CONTROL_FILE=$5
LOCAL_DIR=$6
WRK_DIR=$7
FTP_DETAILS=$8

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`

cd $WRK_DIR
execution_date=`date +"%y%m%d"`
execution_month=`date +"%m"`

rm -f $WRK_DIR/*


#######################################
# Importing file list from FTP server #
#######################################

sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WRK_DIR/wrk_pre_ftp_output_file_1.txt
cd $FTP_DIR
ls -l
quit
END_SCRIPT

if [ -s $WRK_DIR/wrk_pre_ftp_output_file_1.txt ]
  then
  echo "FTP connection successful" >> $WRK_DIR/PRISM_log.txt
else
  New_files_not_available="PRISM | FTP connection failed"
  echo Mail_MSG=$New_files_not_available
  echo "ERRORVAL=FALSE"
  echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
  exit 0;
fi

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }'  | sed '$d' > $WRK_DIR/ftp_files_incremental.txt

if [ -s $WRK_DIR/ftp_files_incremental.txt ]
  then
  echo "New files available" >> $WRK_DIR/PRISM_log.txt
else
  New_files_not_available="PRISM | Files are not available in FTP. Please check!"
  echo Mail_MSG=$New_files_not_available
  echo "ERRORVAL=FALSE"
  echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
  exit 0;
fi

File_list=`cat $WRK_DIR/ftp_files_incremental.txt | xargs`
FTP_File_count=`cat $WRK_DIR/ftp_files_incremental.txt | wc -l`


#################################
# Validate file name convention #
#################################

# Current file convention: "PrismCorporateTracking_<mm>*.zip" (case insensitive) #

# First check: PrismCorporateTracking*.zip #
while read -r line
do
echo "Matching convention: PrismCorporateTracking*.zip" >> $WRK_DIR/PRISM_log.txt
  if [[ $line = PrismCorporateTracking*zip ]] ;
    then
    echo "Matching file found: $line" >> $WRK_DIR/PRISM_log.txt
    varp=${line:0:23}
    varm=${line:23:4}
    vary=${line:27:4}
    varz=${line:31:4}
    echo "$varp$vary$varm$varz" >> $WRK_DIR/prism_sort_list.txt
  else
    echo "Non-matching file found: $line" >> $WRK_DIR/PRISM_log.txt
  fi
done < $WRK_DIR/ftp_files_incremental.txt

if [ -s $WRK_DIR/prism_sort_list.txt ]
  then
  prospective_count=`cat $WRK_DIR/prism_sort_list.txt | wc -l`
  echo "$prospective_count PRISM file(s) found." >> $WRK_DIR/PRISM_log.txt
else
  echo "Mail_MSG=PRISM | No Matching Files Found."
  echo "BODY1=No files found that match pattern PrismCorporateTracking_${execution_month}ddyyyy.zip. Please check FTP."
  echo "ERRORVAL=FALSE"
  echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
  exit 0;
fi

# Sortlist contains filenames with dates switched from MMDDYYYY to YYYYMMDD #
sort $WRK_DIR/prism_sort_list.txt -o $WRK_DIR/prism_sort_list.txt

# Second check: 0 < MM < 13, ensuring acceptable month value #
# Select file with earliest date fitting filename conventions #
while read -r line
do
  varp=${line:0:23}
  vary=${line:23:4}
  varm=${line:27:2}
  vard=${line:29:2}
  varz=${line:31:4}
  if [[ $varm -gt 0 ]] && [[ $varm -lt 13 ]] ;
    then
    echo "Earliest PRISM file found: $varp$varm$vard$vary$varz" >> $WRK_DIR/PRISM_log.txt
    echo "$varp$varm$vard$vary$varz" >> $WRK_DIR/file_to_download.txt
    break
  else
    echo "Non-matching file found: $line" >> $WRK_DIR/PRISM_log.txt
  fi
done < $WRK_DIR/prism_sort_list.txt

if [ -s $WRK_DIR/file_to_download.txt ]
  then
  echo "Beginning ingestion...">> $WRK_DIR/PRISM_log.txt
else
  echo "Mail_MSG=PRISM | No Matching Files Found."
  echo "BODY1=No files found that match pattern PrismCorporateTracking_${execution_month}ddyyyy.zip. Please check FTP."
  echo "ERRORVAL=FALSE"
  echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
  exit 0;
fi


##################################
# Download files from FTP server #
##################################

cd $LOCAL_DIR
rm -f $LOCAL_DIR/*

cat $WRK_DIR/file_to_download.txt | while read line;
do
sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WRK_DIR/ftp_log.txt
cd $FTP_DIR
mget $line
quit
END_SCRIPT
done

if grep -q "Not logged in" $WRK_DIR/ftp_log.txt;
  then
  FTP_ISSUE="PRISM | FTP connection failed"
  echo Mail_MSG=$FTP_ISSUE
  echo "ERRORVAL=FALSE"
  echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
#  exit 0;
else
  echo "FTP connection successful"
fi


##########################
# Unzip downloaded files #
##########################

cat $WRK_DIR/file_to_download.txt | while read line;
do
  filename=$line
  unzip -j $LOCAL_DIR/$filename
  sed -i 's/\r$//' $line
  sed -i -e "1d" $line
done


######################################################
# Echo number of files downloaded/available in souce #
######################################################

# Disabled - downloading files as single archive.
# Local_file_count=`ls -ltr * | wc -l`
# echo "$Local_file_count"
# echo "$FTP_File_count"


################################################
# Validate naming convention of unzipped files #
################################################

ls -lrt | tr 'A-Z' 'a-z' >> $WRK_DIR/local_file_list.txt

if grep -q prism-corporatemarketshare $WRK_DIR/local_file_list.txt;
  then
  if grep -q prism-corporatetickettracking $WRK_DIR/local_file_list.txt;
    then
    if grep -q prism_corpmktshrcntrl $WRK_DIR/local_file_list.txt;
      then
      if grep -q prism_corptkttrkcntrl $WRK_DIR/local_file_list.txt;
        then
        echo "all files exist"
      else
        New_files_not_available="PRISM | .zip Archive Incomplete"
        echo Mail_MSG=$New_files_not_available
        echo "BODY1=Failure Notification: PRISM ingestion file missing from .zip archive (prism_corptkttrkcntrl)"
        echo "ERRORVAL=FALSE"
        echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
        exit 0
      fi
    else
      New_files_not_available="PRISM | .zip Archive Incomplete"
      echo Mail_MSG=$New_files_not_available
      echo "BODY1=Failure Notification: PRISM ingestion file missing from .zip archive (prism_corpmktshrcntrl)"
      echo "ERRORVAL=FALSE"
      echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
      exit 0
    fi
  else
      New_files_not_available="PRISM | .zip Archive Incomplete"
    echo Mail_MSG=$New_files_not_available
    echo "BODY1=Failure Notification: PRISM ingestion file missing from .zip archive (prism-corporatetickettracking)"
    echo "ERRORVAL=FALSE"
    echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
    exit 0
  fi
else
      New_files_not_available="PRISM | .zip Archive Incomplete"
  echo Mail_MSG=$New_files_not_available
  echo "BODY1=Failure Notification: PRISM ingestion file missing from .zip archive (prism-corporatemarketshare)"
  echo "ERRORVAL=FALSE"
  echo "$execution_date;FAILED" | hdfs dfs -appendToFile - ${CONTROL_FILE}
  exit 0
fi


#############################
# Creating HDFS directories #
#############################

hadoop fs -test -d ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShare}
if [ $? == 0 ]; then
  echo "exists" >> $WRK_DIR/PRISM_log.txt
  hdfs dfs -rm -r ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShare}
  hdfs dfs -mkdir -p ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShare}
else
  hdfs dfs -mkdir -p ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShare}
fi

hadoop fs -test -d ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTracking}
if [ $? == 0 ]; then
  echo "exists" >> $WRK_DIR/PRISM_log.txt
  hdfs dfs -rm -r ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTracking}
  hdfs dfs -mkdir -p ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTracking}
else
  hdfs dfs -mkdir -p ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTracking}
fi

hadoop fs -test -d ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShareControlFile}
if [ $? == 0 ]; then
  echo "exists" >> $WRK_DIR/PRISM_log.txt
  hdfs dfs -rm -r ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShareControlFile}
  hdfs dfs -mkdir -p ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShareControlFile}
else
  hdfs dfs -mkdir -p ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShareControlFile}
fi

hadoop fs -test -d ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTrackerControlFile}
if [ $? == 0 ]; then
  echo "exists" >> $WRK_DIR/PRISM_log.txt
  hdfs dfs -rm -r ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTrackerControlFile}
  hdfs dfs -mkdir -p ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTrackerControlFile}
else
  hdfs dfs -mkdir -p ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTrackerControlFile}
fi


########################
# Moving files to HDFS #
########################

ls -ltr * | awk '{ print $9 }' | sed '/.zip/d'  | while read line;
do
  if [ $line = PRISM-CorporateMarketShare* ] ;
    then
    gzip $line
    hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShare}
  else
    if [ $line = PRISM-CorporateTicketTracking* ] ;
      then
      gzip $line
      hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTracking}
    else
      if [ $line = prism_corpmktshrcntrl* ] ;
        then
        gzip $line
        hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShareControlFile}
      else
        if [ $line = prism_corptkttrkcntrl* ] ;
          then
          gzip $line
          hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTrackerControlFile}
        else
          echo "Ignoring file: $line" >> $WRK_DIR/PRISM_log.txt
        fi
      fi
    fi
  fi
done


###########################
# Creating _SUCCESS flags #
###########################

hdfs dfs -touchz ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShare}/_SUCCESS
hdfs dfs -touchz ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTracking}/_SUCCESS
hdfs dfs -touchz ${HDFS_TGT_DIR_Manual_PRISM_CorporateMarketShareControlFile}/_SUCCESS
hdfs dfs -touchz ${HDFS_TGT_DIR_Manual_PRISM_CorporateTicketTrackerControlFile}/_SUCCESS


#######################
# Updating audit file #
#######################

# Note - updateing control file with YYYY-MM-DD (i.e. correct) date format. Filename is MMDDYYYY. #
file_date=`cat $WRK_DIR/file_to_download.txt | awk '
BEGIN {OFS="-"}
{print substr($0,28,4), substr($0,24,2), substr($0,26,2)}'`
execution_date=`date +"%y%m%d"`
echo "$execution_date;SUCCESS;$file_date" | hdfs dfs -appendToFile - ${CONTROL_FILE}

exit 0
