#!/bin/bash

# -----------------------------------------------
#
#       Developer:         Satish Reddy (S751237)
#       Purpose:          oag_ftp_Script identifies the incremental files and copies them to HDFS
#
# -----------------------------------------------




HDFS_DIR_TARGET=$1
CONTROL_FILE=$2
LOCAL_DIR=$3
WRK_DIR=$4
FTP_DETAILS=$5

export HADOOP_USER_NAME=scv_ops

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`

#HDFS_DIR_TYPE1='/data/helix/raw/oag/type1/incremental/11'
#HDFS_DIR_TYPE2='/data/helix/raw/oag/type2/incremental/11'
#HDFS_DIR_TYPE3='/data/helix/raw/oag/type3/incremental/11'
#HDFS_DIR_TYPE4='/data/helix/raw/oag/type4/incremental/11'
#CONTROL_FILE='/apps/helix/controlfiles/oag/oag_control.txt'
#LOCAL_DIR='/data/1/temp/oag'
#WRK_DIR='/apps/helix/scripts/oag'

#FTP_HOST=ftp1.emirates.com
#FTP_USER=FTIROPS02
#FTP_PASSWORD=BfW4%qw9
#FTP_DIR='/SSIM-MCT'
#WRK_DIR='/apps/helix/scripts/oag'
#HDFS_PATH='/data/helix/temp/raw/midt/midtpnr/incremental'


#CONTROL_FILE=$5

#CONTROL_FILE='/apps/helix/controlfiles/oag/oag_control.txt'

#finding last processed date from control file
##############################################

#LAST_PROCESSED_DATE=`hdfs dfs -cat ${CONTROL_FILE}| tail -1| cut -d";" -f1`


cd $WRK_DIR
rm -f ftp_files_incremental.txt $WRK_DIR/ftp_log $WRK_DIR/wrk_pre_ftp_output_file_1.txt 

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
       New_files_not_available="OAG| FTP connection failed"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"

        exit 0;
fi


cat wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d' > ftp_files_incremental.txt



regexp='^[0-9]+$'

#while read -r line
#do
# 
#				 var11=${line:0:4}
#				  var12=${line:4:1}
#				  var13=${line:5:2}
#				   var14=${line:10:2}
#				 #&& [[ $var13 =~ $regexp ]] && [[ $var14 =~ $regexp ]]  && [[ $var12 = _ ]] 
#				   if  [[ $var11 != ssim ]] || [[ $var12 != _ ]] || ! [[ $var13 =~ $regexp ]] || ! [[ $var14 =~ $regexp ]];
#				   
#		         	 then
#		              FILE_NAME="OAG | File name convention is incorrect.Please check! "
#					  echo "BODY1=Actual Filename:$line ; Expected Filename:ssim_yymondd.dat"
#				      echo Mail_MSG=$FILE_NAME
#                      echo "ERRORVAL=FALSE"
#				      exit 0;
#				else
#				     echo "passed"
#				fi
#		
#done < wrk_full_ftp_file_list.txt 


#check and populate new files to ftp_files_incremental.txt
##########################################################

#cat wrk_full_ftp_file_list.txt | while read line;
#do
#var1=${line:5:2}
#month=${line:7:3}
#var2=`date -d "1 $month" "+%m"`
#var3=${line:10:2}
#if [ "$var1$var2$var3" -gt "$LAST_PROCESSED_DATE" ]
#then
#echo $line >> ftp_files_incremental.txt
#fi

#done

chmod 777 $WRK_DIR/*

if [ -s ftp_files_incremental.txt ]
then
        echo "New files available"
else
      New_files_not_available="OAG File Data ingestion – Failure Notification"
            echo Mail_MSG=$New_files_not_available
echo "BODY1=FTP files does not exists in FTP server"
                echo "ERRORVAL=FALSE"

                  exit 0;
fi



File_list=`cat ftp_files_incremental.txt | xargs`
FTP_File_count=`cat ftp_files_incremental.txt | wc -l`

echo FTP_FILES=$File_list


while read -r line
do
  if [ $line = SSIM.txt ]  ;
        then
            echo "file naming convention is correct"
			
			else 
			FILE_NAME="OAG  | File name convention is incorrect.Please check! "
					  echo "BODY1=Actual Filename:$line ; Expected Filename:SSIM.txt"
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
LOCAL_FILES=`ls -ltr * | awk '{ print $9 }' | xargs `
echo "$LOCAL_FILES"

#comparing number of files in ftp server and local
##################################################

if [ "$Local_file_count" -ne "$FTP_File_count" ]
then
      FTP_mismatch="OAG| FTP of few files is not successfull. Please check!"
          echo "ERRORVAL=FALSE"
           echo Mail_MSG=$FTP_mismatch
		    exit 0;
      #`echo "Please check!"| mail -s "MIDT | FTP of few files is not successfull" satish.sathambakam@dnata.com`
 fi

#unzip -j '*.zip'


#############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_DIR_TARGET}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_DIR_TARGET}

else
    echo "do nothing"
fi

############################################
#Creating directory and moving files to HDFS
############################################
hadoop fs -test -d ${HDFS_DIR_TARGET}

if [ $? == 0 ]; then
    echo "exists"

else
    hdfs dfs -mkdir -p ${HDFS_DIR_TARGET}
fi





         cat $WRK_DIR/ftp_files_incremental.txt  | while read line;
do



  
             if [[ $line = SS* ]];
        then
             gzip $line
             hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_DIR_TARGET}

            			 			 else
			 echo "do nothing"
			 fi

execution_date=`date +"%y%m%d"`
echo "$execution_date;SUCCESS" | hdfs dfs -appendToFile - ${CONTROL_FILE}
	
	
done

 hdfs dfs -touchz ${HDFS_DIR_TARGET}/_SUCCESS
 


exit 0
