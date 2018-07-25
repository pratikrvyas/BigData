#*********************************************************
#reconcilation of all countries on the last run of each month
#*********************************************************

CONTROL_FILE=$1
log_file=$2
LOCAL_DIR=$3
WRK_DIR=$4
COUNTRY_LIST=$5
HDFS_WRK_DIR=$6

rm -f $WRK_DIR/acl_qf_control.txt
rm -f $WRK_DIR/acl_qf_country_list.txt
rm -f $WRK_DIR/month_loaded_countries
rm -f $WRK_DIR/countries_processed.txt
rm -f $WRK_DIR/countries_not_processed.txt

yearmonth=`date +"%Y%m"`
timestamp=`date +"%F %T" `
datadate=$(date +"%Y%m" -d "`date +"%Y%m15"` -1 month")
echo yearmonth="$yearmonth"
echo timestamp="$timestamp"
echo datadate="$datadate"
hdfs dfs -copyToLocal ${CONTROL_FILE} $WRK_DIR 
hdfs dfs -copyToLocal ${COUNTRY_LIST} $WRK_DIR 
chmod 777 $WRK_DIR/*
cat $WRK_DIR/acl_qf_control.txt| sed -n "/$datadate/p" | awk -F ";" '{ print $2 }' >> $WRK_DIR/month_loaded_countries


echo "Report Generation Date: $timestamp"$'\n'"Month of Data: $datadate"$'\n'"==============================================" >> $WRK_DIR/countries_processed.txt

echo "Report Generation Date: $timestamp"$'\n'"Month of Data: $datadate"$'\n'"==============================================" >> $WRK_DIR/countries_not_processed.txt

processed_count=0;
not_processed_count=0;
proc_flag=0;
unproc_flag=0;

cut -c1-3 $WRK_DIR/acl_qf_country_list.txt | { while read line;
do
  if  grep -q $line $WRK_DIR/month_loaded_countries ;
  then
    echo "$line" >> $WRK_DIR/countries_processed.txt
    processed_count=$((processed_count+1));
    proc_flag=1;
  else
    echo "$line" >> $WRK_DIR/countries_not_processed.txt
    not_processed_count=$((not_processed_count+1));
    unproc_flag=1;
  fi
done

if [ $proc_flag -eq 1 ]
then
  echo "Some country files found for this month."
else
  echo "No country files have been processed for this month"
  echo "None - No country files have been processed for this month." >> $WRK_DIR/countries_processed.txt
fi

if [ $unproc_flag -eq 1 ]
then
  echo "Some country files are missing for this month"
else
  echo "All country files are processed for this month"
  echo "None - all countries processed successfully." >> $WRK_DIR/countries_not_processed.txt
fi

echo "=============================================="$'\n'"Countries processed: $processed_count" >> $WRK_DIR/countries_processed.txt

echo "=============================================="$'\n'"Countries not processed: $not_processed_count" >> $WRK_DIR/countries_not_processed.txt

echo "processed_count"="$processed_count";
echo "not_processed_count"="$not_processed_count";
}

hadoop fs -test -d ${HDFS_WRK_DIR}/Reports/
if [ $? == 0 ]; then
  echo "${HDFS_WRK_DIR}/Reports/ exists."
else
  echo "${HDFS_WRK_DIR}/Reports/ does not exist."
  hdfs dfs -mkdir ${HDFS_WRK_DIR}/Reports/
fi

hadoop fs -test -f ${HDFS_WRK_DIR}/Reports/$datadate'_countries_processed.txt'
if [ $? == 0 ]; then
  echo "Exists: ${HDFS_WRK_DIR}/Reports/$datadate'_countries_processed.txt'"
  hdfs dfs -rm -r ${HDFS_WRK_DIR}/Reports/$datadate'_countries_processed.txt'
  hdfs dfs -copyFromLocal $WRK_DIR/countries_processed.txt ${HDFS_WRK_DIR}/Reports/$datadate'_countries_processed.txt'
else
  echo "Does not exist: ${HDFS_WRK_DIR}/Reports/$datadate'_countries_processed.txt'"
  hdfs dfs -copyFromLocal $WRK_DIR/countries_processed.txt ${HDFS_WRK_DIR}/Reports/$datadate'_countries_processed.txt'
fi

hadoop fs -test -f ${HDFS_WRK_DIR}/Reports/$datadate'_countries_not_processed.txt'
if [ $? == 0 ]; then
  echo "Exists: ${HDFS_WRK_DIR}/Reports/$datadate'_countries_not_processed.txt'"
  hdfs dfs -rm -r ${HDFS_WRK_DIR}/Reports/$datadate'_countries_not_processed.txt'
  hdfs dfs -copyFromLocal $WRK_DIR/countries_not_processed.txt ${HDFS_WRK_DIR}/Reports/$datadate'_countries_not_processed.txt'
else
  echo "Does not exist: ${HDFS_WRK_DIR}/Reports/$datadate'_countries_not_processed.txt'"
  hdfs dfs -copyFromLocal $WRK_DIR/countries_not_processed.txt ${HDFS_WRK_DIR}/Reports/$datadate'_countries_not_processed.txt'
fi

hadoop fs -chmod 755 ${HDFS_WRK_DIR}/Reports/$datadate'_countries_processed.txt'
hadoop fs -chmod 755 ${HDFS_WRK_DIR}/Reports/$datadate'_countries_not_processed.txt'

job_rc=$?
if [[ $job_rc -ne 0 ]];
then
  exit 6;
else 
  echo "no error'"
fi

exit 0