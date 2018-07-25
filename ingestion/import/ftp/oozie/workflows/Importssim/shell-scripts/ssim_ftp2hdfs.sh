#!/bin/bash

# -----------------------------------------------
#
#       Developer:         Satish Reddy (S751237)
#       Purpose:          ssim_ftp_Script identifies the incremental files and copies them to HDFS
#
# -----------------------------------------------




HDFS_DIR_MKTG=$1
CONTROL_FILE=$2
LOCAL_DIR=$3
WRK_DIR=$4
FTP_DETAILS=$5
HDFS_DIR_OPR=$6



FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`

export HADOOP_USER_NAME=scv_ops

#FTP_HOST=ftp1.emirates.com
#FTP_USER=insight
#FTP_PASSWORD=!ns!ght
#FTP_DIR='/incoming/insight/SSIM'


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

LAST_PROCESSED_DATE=`hdfs dfs -cat ${CONTROL_FILE}| tail -1 | cut -d";" -f1`


cd $WRK_DIR
rm -f $WRK_DIR/ftp_files_incremental.txt $WRK_DIR/ftp_log $WRK_DIR/wrk_full_ftp_file_list.txt  $WRK_DIR/wrk_pre_ftp_output_file_1.txt 

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
       New_files_not_available="SSIM  | FTP connection failed"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"

        exit 0;
fi


cat wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d' > $WRK_DIR/wrk_full_ftp_file_list.txt


regexp='^[0-9]+$'

while read -r line
do
             if [[ $line = ssim_ek_mktg* ]];
                then
		     
				  var11=${line:0:4}
				  var12=${line:4:1}
				  var13=${line:5:2}
                  var14=${line:7:1}
				  var15=${line:8:4}
				  var16=${line:12:1}
				  var17=${line:13:2}
                  var18=${line:18:2}
				  var19=${line:20:1}
				  dot='.'
				 #&& [[ $var13 =~ $regexp ]] && [[ $var14 =~ $regexp ]]  && [[ $var12 = _ ]] 
				   if  [[ $var11 != ssim ]] || [[ $var12 != _ ]] ||  [[ $var13 != ek ]] ||  [[ $var14 != _ ]] ||  [[ $var15 != mktg ]] ||  [[ $var16 != _ ]] || ! [[ $var17 =~ $regexp ]] || ! [[ $var18 =~ $regexp ]] ||  [[ $var19 != $dot ]] ;
				   
		         	 then
		              FILE_NAME="SSIM | File name convention is incorrect.Please check! "
					  echo "BODY1=Actual Filename:$line ; Expected Filename:ssim_ek_mktg_ddmonyy.sds/ssim_ek_ddmonyy.sds"
				      echo Mail_MSG=$FILE_NAME
                      echo "ERRORVAL=FALSE"
				      exit 0;
				else
				     echo "passed"
				fi
		     else
			 
			      var21=${line:0:4}
				  var22=${line:4:1}
				  var23=${line:5:2}
                  var24=${line:7:1}
				    var27=${line:8:2}
                 var28=${line:13:2}
				 var29=${line:15:1}
				 dot1='.'
				 #&& [[ $var13 =~ $regexp ]] && [[ $var14 =~ $regexp ]]  && [[ $var12 = _ ]] 
				   if  [[ $var21 != ssim ]] || [[ $var22 != _ ]] ||  [[ $var23 != ek ]] ||  [[ $var24 != _ ]] || ! [[ $var27 =~ $regexp ]] || ! [[ $var28 =~ $regexp ]] ||  [[ $var29 != $dot1 ]];
				   
		         	 then
		              FILE_NAME="SSIM | File name convention is incorrect.Please check! "
					  echo "BODY1=Actual Filename:$line ; Expected Filename:ssim_ek_mktg_ddmonyy.sds/ssim_ek_ddmonyy.sds"
				      echo Mail_MSG=$FILE_NAME
                      echo "ERRORVAL=FALSE"
				      exit 0;
				else
				     echo "passed"
				fi
		     fi
		done < $WRK_DIR/wrk_full_ftp_file_list.txt 

hdfs dfs -copyToLocal ${CONTROL_FILE} $WRK_DIR

#check and populate new files to ftp_files_incremental.txt
##########################################################

cat $WRK_DIR/wrk_full_ftp_file_list.txt | while read line;
do 
   if [[ $line = ssim_ek_mktg* ]];
                then
                    var1=${line:13:2}
                    month=${line:15:3}
                    var2=`date -d "1 $month" "+%m"`
                    var3=${line:18:2}
                                  if [ "$var3$var2$var1" -ge "$LAST_PROCESSED_DATE" ]
                                  then
								  
				                  	var32='EK_MKTG_SSIM'
				                   pattern=$var3$var2$var1';'$var32
                                    echo "$pattern"
				                               if grep -q $pattern $WRK_DIR/ssim_control.txt;
                                               then
				                               echo "Already processed in last run"
				                               else
                                              echo $line >> ftp_files_incremental.txt
                                              fi
				                    fi
	else 
                    var111=${line:8:2}
                    month111=${line:10:3}
                    var222=`date -d "1 $month111" "+%m"`
                    var333=${line:13:2}
                                  if [ "$var333$var222$var111" -ge "$LAST_PROCESSED_DATE" ]
                                  then
								  
				                  	var3232='EK_OPR_SSIM'
				                   pattern=$var333$var222$var111';'$var3232
                                    echo "$pattern"
				                               if grep -q $pattern $WRK_DIR/ssim_control.txt;
                                               then
				                               echo "Already processed in last run"
				                               else
                                              echo $line >> ftp_files_incremental.txt
                                              fi
	
			                    fi
	fi
done

rm -f $WRK_DIR/ssim_control.txt

chmod 777 $WRK_DIR/*

if [ -s ftp_files_incremental.txt ]
then
        echo "New files available"
else
       New_files_not_available="SSIM File Data ingestion – Failure Notification"
            echo Mail_MSG=$New_files_not_available
echo "BODY1=FTP files does not exists in FTP server"
                echo "ERRORVAL=FALSE"

                  exit 0;
fi



File_list=`cat ftp_files_incremental.txt | xargs`
FTP_File_count=`cat ftp_files_incremental.txt | wc -l`

echo FTP_FILES=$File_list

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


Local_file_count=`ls -ltr  ss* | wc -l`
LOCAL_FILES=`ls -ltr ss* | awk '{ print $9 }' | xargs `
echo "$LOCAL_FILES"

#comparing number of files in ftp server and local
##################################################

if [ "$Local_file_count" -ne "$FTP_File_count" ]
then
      FTP_mismatch="SSIM| FTP of few files is not successfull. Please check!"
          echo "ERRORVAL=FALSE"
           echo Mail_MSG=$FTP_mismatch
		    exit 0;
      #`echo "Please check!"| mail -s "MIDT | FTP of few files is not successfull" satish.sathambakam@dnata.com`
 fi

#unzip -j '*.zip'
 #############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_DIR_MKTG}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_DIR_MKTG}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_DIR_OPR}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_DIR_OPR}

else
    echo "do nothing"
fi



############################################
#Creating directory and moving files to HDFS
############################################
hadoop fs -test -d ${HDFS_DIR_MKTG}

if [ $? == 0 ]; then
    echo "exists"

else
    hdfs dfs -mkdir -p ${HDFS_DIR_MKTG}
fi



hadoop fs -test -d ${HDFS_DIR_OPR}

if [ $? == 0 ]; then
    echo "exists"

else
    hdfs dfs -mkdir -p ${HDFS_DIR_OPR}
fi



         cat $WRK_DIR/ftp_files_incremental.txt  | while read line;
do

              if [[ $line = ssim_ek_mktg* ]];
                then
                                           gzip $line
                                           hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_DIR_MKTG}
                              
                 else                         
              if [[ $line = ssim_ek_* ]];
                then                           			  
                                 
                                           gzip $line
                                           hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_DIR_OPR}
										   
				else
				echo "do nothing"
                              
                   fi                       
				fi 
	
done

cat $WRK_DIR/ftp_files_incremental.txt  | while read line;
do

              if [[ $line = ssim_ek_mktg* ]];
			  then
			  var100=${line:13:2}
                    month12=${line:15:3}
                    var200=`date -d "1 $month12" "+%m"`
                    var300=${line:18:2}
                    processing_date=$var300$var200$var100
                    execution_date=`date +"%y%m%d"`
                    echo "$processing_date;EK_MKTG_SSIM;$execution_date" | hdfs dfs -appendToFile - ${CONTROL_FILE}
					
					
				else
				    var1110=${line:8:2}
                    month1=${line:10:3}
                    var2220=`date -d "1 $month1" "+%m"`
                    var3330=${line:13:2}
					processing_date=$var3330$var2220$var1110
                    execution_date=`date +"%y%m%d"`
                    echo "$processing_date;EK_OPR_SSIM;$execution_date" | hdfs dfs -appendToFile - ${CONTROL_FILE}
					
				fi
				
	done

 hdfs dfs -touchz ${HDFS_DIR_MKTG}/_SUCCESS

 
 hdfs dfs -touchz ${HDFS_DIR_OPR}/_SUCCESS


exit 0
