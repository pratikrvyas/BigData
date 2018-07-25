# ----------------------------------------------------------------
#
#       Developer:         Mohan Selvamani/Satish Reddy (S751093, S751237)
#       Purpose:          Acl_ftp_ Script identifies the incremental files and copies them to HDFS
#
# ------------------------------------------------------------------

#CONTROL_FILE='/apps/helix/scripts/accelya/acl_control.txt'


HDFS_DIR_R5117=$1
HDFS_DIR_T5115=$2
HDFS_DIR_CNTRL=$3

HDFS_REJECTED_DIR_R5117=$4
HDFS_REJECTED_DIR_T5115=$5
HDFS_REJECTED_DIR_CNTRL=$6

CONTROL_FILE=$7
log_file=$8
LOCAL_DIR=$9
WRK_DIR=${10}
LAST_RUN_DATE=${11}

FTP_DETAILS=${12}
COUNTRY_LIST=${13}


export HADOOP_USER_NAME=scv_ops

#FTP_HOST=ftp1.emirates.com
#FTP_USER=insight
#FTP_PASSWORD=!ns!ght
#FTP_DIR='/incoming/insight/ACL_TEST' # NEED TO CHANGE ACTUAL LOCATION

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`




#*********************************************************
#finding last processed date from control file
#*********************************************************

LAST_PROCESSED_MONTH=`hdfs dfs -cat ${CONTROL_FILE}| tail -1| cut -d";" -f1`


cd $WRK_DIR
rm -f $WRK_DIR/wrk_full_ftp_file_list.txt  $WRK_DIR/ftp_files_incremental.txt $WRK_DIR/incremental_country.txt $WRK_DIR/ftp_log $WRK_DIR/wrk_pre_ftp_output_file_1.txt

#*********************************************************
#importing file list from FTP SERVER
#*********************************************************

sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WRK_DIR/wrk_pre_ftp_output_file_1.txt 
cd $FTP_DIR
ls -l
quit
END_SCRIPT

if [ -s $WRK_DIR/wrk_pre_ftp_output_file_1.txt ]
then
        echo "FTP connection successful"
else
       New_files_not_available="Accelya| FTP connection failed"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"

        exit 0;
fi

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d' > wrk_full_ftp_file_list.txt



#*********************************************************
#check and populate new files to ftp_files_incremental.txt
#*********************************************************

cat wrk_full_ftp_file_list.txt | while read line; 
do
var1=${line:13:6}
if [ "$var1" -ge "$LAST_PROCESSED_MONTH" ]
then
  var2=${line:6:2}
  pattern=$var1';'$var2
  echo "$pattern"
  hdfs dfs -copyToLocal ${CONTROL_FILE} $WRK_DIR 
  if grep -q $pattern $WRK_DIR/acl_control.txt;
   then
   echo "Already processed in last run"
   else
   echo $line >> ftp_files_incremental.txt
   fi
fi


job_rc=$?
 if [[ $job_rc -ne 0 ]];
then
exit 2;
else 
echo "no error'"
fi

done

rm -f $WRK_DIR/acl_control.txt

chmod 777 $WRK_DIR/*

if [ -s ftp_files_incremental.txt ]
then
        echo "New files available"
else
       New_files_not_available="ACCELYA | Files are not available. Please check!"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"


        exit 0;
fi

File_list=`cat ftp_files_incremental.txt | xargs`


echo FTP_FILES=$File_list
cd $LOCAL_DIR

rm -f $LOCAL_DIR/*

#*********************************************************
#importing new files to local directory
#*********************************************************

cat $WRK_DIR/ftp_files_incremental.txt | while read line;
do
sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WRK_DIR/ftp_log 
cd $FTP_DIR
mget $line
quit
END_SCRIPT

done

echo "LOCAL_FILES=`ls -ltr r511x* | awk '{ print $9 }' | xargs `"

Local_file_count=`ls -ltr $LOCAL_DIR/r511x* | wc -l`

FTP_File_count=`cat $WRK_DIR/ftp_files_incremental.txt | wc -l`
#*********************************************************
#comparing number of files in ftp server and local
#*********************************************************
if [ "$Local_file_count" -ne "$FTP_File_count" ]
then
FTP_mismatch="ACCELYA | FTP of few files is not successfull. Please check!"
          		  echo Mail_MSG=$FTP_mismatch
				  echo "ERRORVAL=FALSE"
				  exit 0;

fi


#############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_DIR_R5117}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_DIR_R5117}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_DIR_T5115}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_DIR_T5115}
        else
     echo "do nothing"
fi

hadoop fs -test -d ${HDFS_DIR_CNTRL}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_DIR_CNTRL}

else
 echo "do nothing"
fi

hadoop fs -test -d ${HDFS_REJECTED_DIR_R5117}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_REJECTED_DIR_R5117}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_REJECTED_DIR_T5115}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_REJECTED_DIR_T5115}
        else
     echo "do nothing"
fi

hadoop fs -test -d ${HDFS_REJECTED_DIR_CNTRL}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_REJECTED_DIR_CNTRL}

else
 echo "do nothing"
fi



 cat $WRK_DIR/ftp_files_incremental.txt | while read line;
do
       
                 var1=${line:6:2}
				 var2=${line:13:6}
				 echo $var1*$var2 >> $WRK_DIR/incremental_country.txt
                 
         

done

while read -r line
do

filename=$line
cntry=${line:0:2}
processing_date=${line:3:6}
unzip -j *$filename*.zip

HDFS_R5117=$HDFS_DIR_R5117
HDFS_T5115=$HDFS_DIR_T5115
HDFS_CNTRL=$HDFS_DIR_CNTRL
regexp='^[0-9]+$'
execution_date=`date +"%y%m%d"`

r5117=`ls -ltr *r5117*$filename* | awk '{ print $9 }'` 
t5115=`ls -ltr *t5115*$filename* | awk '{ print $9 }'` 
cntrl_filename=`ls -ltr CTRL*$filename* | awk '{ print $9 }'` 

control_file_no_of_records=`cat CTRL*$filename* | sed -n "/${r5117}/p" | awk -F ";" '{ print $4 }'`
control_file_ind_net_amnt=`cat CTRL*$filename* | sed -n "/${r5117}/p" | awk -F ";" '{ print $5 }'`
control_file_airline_net_amnt=`cat CTRL*$filename* | sed -n "/${r5117}/p" | awk -F ";" '{ print $6 }'`
control_file_ind_gross_amnt=`cat CTRL*$filename* | sed -n "/${r5117}/p" | awk -F ";" '{ print $7 }'`
control_file_airline_gross_amnt=`cat CTRL*$filename* | sed -n "/${r5117}/p" | awk -F ";" '{ print $8 }'`
control_file_ind_doc_cnt=`cat CTRL*$filename* | sed -n "/${r5117}/p" | awk -F ";" '{ print $9 }'`
control_file_airline_doc_cnt=`cat CTRL*$filename* | sed -n "/${r5117}/p" | awk -F ";" '{ print $10 }'`

no_of_records=`cat $r5117 | wc -l`
ind_net_amnt=`cut -c60-76 $r5117 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
airline_net_amnt=`cut -c76-92 $r5117 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
ind_gross_amnt=`cut -c28-44 $r5117 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
airline_gross_amnt=`cut -c44-60 $r5117 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
ind_doc_cnt=`cut -c92-99 $r5117 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
airline_doc_cnt=`cut -c99-106 $r5117 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`

control_file_no_of_records_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $4 }'`
control_file_ind_net_amnt_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $5 }'`
control_file_airline_net_amnt_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $6 }'`
control_file_ind_gross_amnt_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $7 }'`
control_file_airline_gross_amnt_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $8 }'`
control_file_ind_doc_cnt_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $9 }'`
control_file_airline_doc_cnt_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $10 }'`
control_file_ind_yq_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $11 }'`
control_file_ind_yr_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $12 }'`
control_file_airline_yq_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $13 }'`
control_file_airline_yr_t5115=`cat CTRL*$filename* | sed -n "/${t5115}/p" | awk -F ";" '{ print $14 }'`

no_of_records_t5115=`cat $t5115 | wc -l`
ind_net_amnt_t5115=`cut -c59-75 $t5115 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
airline_net_amnt_t5115=`cut -c75-91 $t5115 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
ind_gross_amnt_t5115=`cut -c27-43 $t5115 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
airline_gross_amnt_t5115=`cut -c43-59 $t5115 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
ind_doc_cnt_t5115=`cut -c91-98 $t5115 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
airline_doc_cnt_t5115=`cut -c98-105 $t5115 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
ind_yq_t5115=`cut -c105-121 $t5115 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
ind_yr_t5115=`cut -c121-137 $t5115 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
airline_yq_t5115=`cut -c137-153 $t5115 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`
airline_yr_t5115=`cut -c153-169 $t5115 | awk '{sum+=$1} END { printf "%.2f\n",sum}'`

 if [ -z $cntrl_filename ]  
               then
			   File_Record_Not_matching="Control file metrics validation failed"
                                       control_file_comp=$File_Record_Not_matching
                                       ERRORVAL=FALSE
									   CONTROL_FLAG=FALSE
               						HDFS_R5117=$HDFS_REJECTED_DIR_R5117
                                       HDFS_T5115=$HDFS_REJECTED_DIR_T5115
                                       HDFS_CNTRL=$HDFS_REJECTED_DIR_CNTRL
               						File_list11=`ls -ltr *$filename*.zip | awk '{ print $9 }' | xargs`
									files_consolidated=$files_consolidated'|'$File_list11
									
									else
                   

         if [ "$(bc <<< "$control_file_no_of_records - $no_of_records ")" = 0  -a  "$(bc <<< "$control_file_ind_net_amnt - $ind_net_amnt")" = 0   -a   "$(bc <<< "$control_file_airline_net_amnt - $airline_net_amnt")" = 0    -a  "$(bc <<< "$control_file_ind_gross_amnt - $ind_gross_amnt")" = 0      -a  "$(bc <<< "$control_file_airline_gross_amnt - $airline_gross_amnt")" = 0  -a "$(bc <<< "$control_file_ind_doc_cnt - $ind_doc_cnt")" = 0    -a "$(bc <<< "$control_file_airline_doc_cnt - $airline_doc_cnt")" = 0 -a "$(bc <<< "$control_file_no_of_records_t5115 - $no_of_records_t5115")" = 0  -a  "$(bc <<< "$control_file_ind_net_amnt_t5115 - $ind_net_amnt_t5115")" = 0   -a   "$(bc <<< "$control_file_airline_net_amnt_t5115 - $airline_net_amnt_t5115")" = 0    -a  "$(bc <<< "$control_file_ind_gross_amnt_t5115 - $ind_gross_amnt_t5115")" = 0        -a  "$(bc <<< "$control_file_airline_gross_amnt_t5115 - $airline_gross_amnt_t5115")" = 0  -a "$(bc <<< "$control_file_ind_doc_cnt_t5115 - $ind_doc_cnt_t5115")" = 0    -a "$(bc <<< "$control_file_airline_doc_cnt_t5115 - $airline_doc_cnt_t5115")" = 0  -a      "$(bc <<< "$control_file_ind_yq_t5115 - $ind_yq_t5115")" = 0   -a  "$(bc <<< "$control_file_ind_yr_t5115 - $ind_yr_t5115")" = 0  -a               "$(bc <<< "$control_file_airline_yq_t5115 - $airline_yq_t5115")" = 0  -a "$(bc <<< "$control_file_airline_yr_t5115 - $airline_yr_t5115")" = 0 ]
     then
         echo "metrics validation successful"

      else
        File_Record_Not_matching="Control file metrics validation failed"
                                       control_file_comp=$File_Record_Not_matching
                                       ERRORVAL=FALSE
									   CONTROL_FLAG=FALSE
               						HDFS_R5117=$HDFS_REJECTED_DIR_R5117
                                       HDFS_T5115=$HDFS_REJECTED_DIR_T5115
                                       HDFS_CNTRL=$HDFS_REJECTED_DIR_CNTRL
               						File_list11=`ls -ltr *$filename*.zip | awk '{ print $9 }' | xargs`
									files_consolidated=$files_consolidated'|'$File_list11
               						
      fi

fi


 ############################################
#Creating directory and moving files to HDFS
############################################

hadoop fs -test -d ${HDFS_R5117}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_R5117}
fi

hadoop fs -test -d ${HDFS_T5115}

if [ $? == 0 ]; then
    echo "exists"
else
    hdfs dfs -mkdir -p ${HDFS_T5115}
fi

hadoop fs -test -d ${HDFS_CNTRL}

if [ $? == 0 ]; then
    echo "exists"
else
    hdfs dfs -mkdir -p ${HDFS_CNTRL}
fi


 gzip $r5117
         hdfs dfs -copyFromLocal ${LOCAL_DIR}/$r5117.gz ${HDFS_R5117}

               gzip $t5115
         hdfs dfs -copyFromLocal ${LOCAL_DIR}/$t5115.gz ${HDFS_T5115}


 gzip CTRL*$filename*
hdfs dfs -copyFromLocal ${LOCAL_DIR}/CTRL*$filename*.gz ${HDFS_CNTRL}


################################################
#updating the control file with the load details
################################################

if [ -z $CONTROL_FLAG ]
then
echo "$processing_date;$cntry;$execution_date" | hdfs dfs -appendToFile - ${CONTROL_FILE}
else
echo "do nothing"
fi

done < $WRK_DIR/incremental_country.txt 

echo "$ERRORVAL"

if [ -z $ERRORVAL ]
then
     echo "validation of all file is successful"
else
 echo "ERRORVAL=FALSE"
echo Mail_MSG="ACCELYA | $files_consolidated Control file metrics validation failed"
echo "$processing_date;$execution_date;$files_consolidated;$File_Record_Not_matching;ACCELYA" | hdfs dfs -appendToFile - ${log_file}
fi

hadoop fs -test -d ${HDFS_DIR_R5117}
if [ $? == 0 ]; then
    echo "exists"
	hdfs dfs -touchz ${HDFS_DIR_R5117}/_SUCCESS
else
    echo "do nothing"	

fi

hadoop fs -test -d ${HDFS_DIR_T5115}

if [ $? == 0 ]; then
    echo "exists"
	hdfs dfs -touchz ${HDFS_DIR_T5115}/_SUCCESS
else
    echo "do nothing"	

fi

#*********************************************************
#reconcilation of all countries on the last run of each month
#*********************************************************

batch_last_day=` date +"%d"`
yearmonth=`date +"%Y%m"`
echo "$batch_last_day $LAST_RUN_DATE"
if [ "$batch_last_day" -eq "$LAST_RUN_DATE" ]
then
hdfs dfs -copyToLocal ${CONTROL_FILE} $WRK_DIR 
hdfs dfs -copyToLocal ${COUNTRY_LIST} $WRK_DIR 
chmod 777 $WRK_DIR/*
  cat $WRK_DIR/acl_control.txt| sed -n "/$yearmonth/p" | awk -F ";" '{ print $2 }' >> $WRK_DIR/month_loaded_countries
cut -c1-3 $WRK_DIR/acl_country_list.txt | { while read line;
do

 if  grep -q $line $WRK_DIR/month_loaded_countries ;
   then
   echo "Country processed"
   else
   MONTH_MISSING_COUNTRIES=$MONTH_MISSING_COUNTRIES';'$line
   fi
done

 if [ -z $MONTH_MISSING_COUNTRIES ]
 then
 echo "all countries files are processed for this month"
 else
 echo "$MONTH_MISSING_COUNTRIES"
`echo "$MONTH_MISSING_COUNTRIES" |mail -s "acl |Did not recieve few country files for this month" "satish.sathambakam@dnata.com,mohan.selvamani@dnata.com,achala.nanayakkara@emirates.com"`
 fi
}
rm -f $WRK_DIR/month_loaded_countries
rm -f $WRK_DIR/acl_control.txt
rm -f $WRK_DIR/acl_country_list.txt
fi

job_rc=$?
 if [[ $job_rc -ne 0 ]];
then
exit 6;
else 
echo "no error'"
fi


exit 0


