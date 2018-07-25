#!/bin/bash

# -----------------------------------------------
#
#       Developer:         Satish Reddy ( S751237)
#       Purpose:         identifies the incremental files and copies them to HDFS
#
# ---------------------------------------------------


HDFS_PATH_MIDTPNR=$1
HDFS_PATH_AGENT_MSTR=$2
HDFS_PATH_PNR_CNTRL=$3
HDFS_PATH_AGENT_CNTRL=$4
CONTROL_FILE=$5
LOCAL_DIR=$6
WRK_DIR=$7
FTP_DETAILS=$8
ERROR_LOG_FILE=$9
HOURCHECK=${10}

export HADOOP_USER_NAME=scv_ops

#HDFS_PATH_MIDTPNR='/data/helix/raw/midt/midt_pnr/incremental/2016-12-1209-30'
#HDFS_PATH_AGENT_MSTR='/data/helix/raw/midt/gds_agent_master/incremental/2016-12-12/09-30'
#HDFS_PATH_PNR_CNTRL='/data/helix/raw/midt/midt_pnr_control_file/incremental/2016-12-12/09-30'
#HDFS_PATH_AGENT_CNTRL='/data/helix/raw/midt/gds_agent_master_control_file/incremental/2016-12-12/09-30'
#CONTROL_FILE='/apps/helix/controlfiles/midt/midt_control.txt'
#LOCAL_DIR='/data/1/share/midt/files'
#WRK_DIR='/data/1/share/midt/work'
#FTP_DETAILS='/home/satishs/midt_ftp_details.txt'
#ERROR_LOG_FILE='/apps/helix/controlfiles/errorlog/error_log.txt'

FTP_HOST=`sed -n '/FTP_HOST/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_USER=`sed -n '/FTP_USER/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_PUBLIC_KEY=`sed -n '/FTP_PUBLIC_KEY/p' $FTP_DETAILS |  cut -d"=" -f2`
FTP_DIR=`sed -n '/FTP_DIR/p' $FTP_DETAILS |  cut -d"=" -f2`


execution_date=`date +"%y%m%d%H%M%S"`
cd $WRK_DIR
rm -f $WRK_DIR/*

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
       New_files_not_available="MIDT| FTP connection failed"
            echo Mail_MSG=$New_files_not_available
                echo "ERRORVAL=FALSE"

        exit 0;
fi

todayrun=`date +"%y%m%d"`
hour=`date +"%H"`
sed '/processed/d' -i  $WRK_DIR/wrk_pre_ftp_output_file_1.txt

regexp='^[0-9]+$'

hdfs dfs -copyToLocal ${CONTROL_FILE} $WRK_DIR 

cat $WRK_DIR/wrk_pre_ftp_output_file_1.txt | sed 1,2d | awk '{ print $9 }' | sed '$d' > $WRK_DIR/wrk_full_ftp_file_list.txt

	  if grep -q _success.txt $WRK_DIR/wrk_full_ftp_file_list.txt;
               then
			   cat $WRK_DIR/wrk_full_ftp_file_list.txt | sed -n -e '/_success.txt/p' | sort > $WRK_DIR/success_flags_list_1.txt
			   
			 else
               if [[ $HOURCHECK = $hour ]];
        then
               if grep -q $todayrun $WRK_DIR/midt_control.txt;
                 then
                 echo "one batch has been already processed"
	             exit 0
			 else
                  New_files_not_available="MIDT | Files are not available. Please check!"
                                echo Mail_MSG=$New_files_not_available
                                 echo "ERRORVAL=FALSE"
                                exit 0	
			  fi
else
      echo "Files are not available"
	  exit 0
fi
              fi 

while read -r line
do

var44=${line:5:1}
var55=${line:6:7}
			echo "$var55$var44;$line" >> $WRK_DIR/success_flags_list_2.txt 	 
		        				
done < $WRK_DIR/success_flags_list_1.txt 


cat $WRK_DIR/success_flags_list_2.txt | sort | awk -F ";" '{ print $2 }' > $WRK_DIR/success_flags_list.txt
rm -f $WRK_DIR/success_flags_list_1.txt $WRK_DIR/success_flags_list_2.txt 			  


			  
while read -r line 
do
var33=${line:0:4}
process_date=${line:4:8}
var44=${line:4:1}
var55=${line:5:7}
var66=${line:12:12}
				 
				 				 
		         if [[ $var33 != midt ]] || [[ $var44 != b ]] ||   [[ $var66 != _success.txt ]] || ! [[ $var55 =~ $regexp ]] ;
								 then
		              echo $line >> wrong_filename_convention.txt
                      				      
				else
					     				
	             if grep -q $process_date $WRK_DIR/midt_control.txt;
               then
			    echo "Already processed"
			 else
              current_batch_date=$var44$var55
			  echo "$current_batch_date"
			  break
			  fi
			  fi
				
done < $WRK_DIR/success_flags_list.txt
	  

if [ -s wrong_filename_convention.txt ]
           then
		   if [ -z $current_batch_date ]
                     then
                            
							  New_files_not_available="MIDT | Success file naming convention is wrong . Please check!"
                                echo Mail_MSG=$New_files_not_available
                                 echo "ERRORVAL=FALSE"
                                exit 0
						
                     else
                              echo "New files available"    
                     fi
		       
		   else
		   

 if [ -z $current_batch_date ]
                     then
                            
							  New_files_not_available="MIDT | Files are not available or existing files are already processed . Please check!"
                                echo Mail_MSG=$New_files_not_available
                                 echo "ERRORVAL=FALSE"
                                exit 0
						
                     else
                              echo "New files available"    
                     fi
					 fi

	  
sed -n -e "/$current_batch_date/p" -i $WRK_DIR/wrk_full_ftp_file_list.txt  
sed '/_success.txt/d' -i  $WRK_DIR/wrk_full_ftp_file_list.txt

while read -r line
do
  if [[ $line = pnragy* ]];
        then
		         var13=${line:0:6}
				 var23=${line:6:1}
				 var33=${line:7:1}
				 var43=${line:8:6}
				 
		         if [[ $var13 != pnragy ]] || [[ $var23 != b ]] || ! [[ $var33 =~ $regexp ]] ||  ! [[ $var43 =~ $regexp ]]  ;
				 then
		              echo $line >> wrong_filename_convention.txt
                      				      
				else
				     pattern=$var23$var33$var43
					  if grep -q $pattern $WRK_DIR/midt_control.txt;
               then
			 echo "Already processed in last run"
			 else
              echo $line >> ftp_files_incremental.txt
              fi 
				     				fi
		else
		         var14=${line:0:3}
				 var24=${line:3:1}
				 var34=${line:4:1}
				 var44=${line:5:6}
				 
				   if [[ $var14 != pnr ]] || [[ $var24 != b ]] ||  ! [[ $var34 =~ $regexp ]] ||  ! [[ $var44 =~ $regexp ]] ;
				 then
				 
				       echo $line >> wrong_filename_convention.txt
		             				      
				else
				pattern1=$var24$var34$var44
				      if grep -q $pattern1 $WRK_DIR/midt_control.txt;
               then
			 echo "Already processed in last run"
			 else
              echo $line >> ftp_files_incremental.txt
              fi
				fi
  fi

             
				
done < $WRK_DIR/wrk_full_ftp_file_list.txt 

rm -f $WRK_DIR/midt_control.txt

    if [ -s wrong_filename_convention.txt ]
           then
			   if [ -s ftp_files_incremental.txt ]
			   then
			                FILE_NAME="MIDT | File name convention is incorrect for few files - skipping them in current process.Please check!"
			            	File_list1=`cat wrong_filename_convention.txt | xargs`
			            	echo "BODY1=Actual Filename: $File_list1 ; Expected Filename:pnrb#yymmdd_nn.zip/pnragyb#yymmdd_nn.zip/pnrb#yymmdd.txt/pnragybyymmdd.txt"
			            	echo Mail_MSG=$FILE_NAME
                            echo "ERRORVAL=FALSE"
                            echo "$execution_date;$FILE_NAME:$File_list1" | hdfs dfs -appendToFile - ${ERROR_LOG_FILE}
                            echo "New files available"
			   else
			                FILE_NAME="MIDT | All the files recieved has wrong file name convention - No new correct files to process.Please check!"
			            	File_list1=`cat wrong_filename_convention.txt | xargs`
			            	echo "BODY1=Actual Filename: $File_list1 ; Expected Filename:pnrb#yymmdd_nn.zip/pnragyb#yymmdd_nn.zip/pnrb#yymmdd.txt/pnragybyymmdd.txt"
			            	echo Mail_MSG=$FILE_NAME
                            echo "ERRORVAL=FALSE"
                            echo "$execution_date;$FILE_NAME:$File_list1" | hdfs dfs -appendToFile - ${ERROR_LOG_FILE}
							exit 0
				fi
          
            else
                     
					 if [ -s ftp_files_incremental.txt ]
                     then
                              echo "New files available. All files naming convention is correct"
						
                     else
                                New_files_not_available="MIDT | Files are not available. Please check!"
                                echo Mail_MSG=$New_files_not_available
                                 echo "ERRORVAL=FALSE"
                                exit 0
                     fi
    fi

	pnrfile=`cat ftp_files_incremental.txt | sed -n -e '/.zip/p' | sed -n -e "/pnr$current_batch_date/p" | sed -n 1p `
	pnragyfile=`cat ftp_files_incremental.txt | sed -n -e '/.zip/p' | sed -n -e "/pnragy$current_batch_date/p" | sed -n 1p `

	if [ -z $pnrfile ] || [ -z $pnragyfile ]
                     then
					 
                            
							  New_files_not_available="MIDT | PNR/Agent files are missing . Please check!"
                                echo Mail_MSG=$New_files_not_available
                                 echo "ERRORVAL=FALSE"
                                exit 0
						
                     else
                              echo "BOTH PNR AND AGENT FILES AVAILABLE"    
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


#############################################
#Deleting target hdfs directories
############################################

hadoop fs -test -d ${HDFS_PATH_MIDTPNR}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_PATH_MIDTPNR}

else
    echo "do nothing"
fi

hadoop fs -test -d ${HDFS_PATH_AGENT_MSTR}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_PATH_AGENT_MSTR}
        else
     echo "do nothing"
fi

hadoop fs -test -d ${HDFS_PATH_PNR_CNTRL}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_PATH_PNR_CNTRL}

else
 echo "do nothing"
fi

hadoop fs -test -d ${HDFS_PATH_AGENT_CNTRL}

if [ $? == 0 ]; then
    echo "exists"
        hdfs dfs -rm -r ${HDFS_PATH_AGENT_CNTRL}

else
    echo "do nothing"
fi

  
 ############################################
#Creating directory and moving files to HDFS
############################################

hadoop fs -test -d ${HDFS_PATH_MIDTPNR}

if [ $? == 0 ]; then
    echo "exists"
	
else
    hdfs dfs -mkdir -p ${HDFS_PATH_MIDTPNR}
fi

hadoop fs -test -d ${HDFS_PATH_AGENT_MSTR}

if [ $? == 0 ]; then
    echo "exists"
else
    hdfs dfs -mkdir -p ${HDFS_PATH_AGENT_MSTR}
fi

hadoop fs -test -d ${HDFS_PATH_PNR_CNTRL}

if [ $? == 0 ]; then
    echo "exists"
else
    hdfs dfs -mkdir -p ${HDFS_PATH_PNR_CNTRL}
fi

hadoop fs -test -d ${HDFS_PATH_AGENT_CNTRL}

if [ $? == 0 ]; then
    echo "exists"
else
    hdfs dfs -mkdir -p ${HDFS_PATH_AGENT_CNTRL}
fi

unzip -j '*.zip'

 ls -ltr * | awk '{ print $9 }' | sed '/.zip/d' | while read line;
 do      
        if [[ $line = pnragyb*.dat ]];
        then
		     
				 batchprocessing=${line:6:8}
				  sed -i 's/\r$//' $line
				 sed -e "s/$/$batchprocessing/" -i $line
				 gzip $line
         hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_PATH_AGENT_MSTR}
                 
          else
               if [[ $line = pnrb*.dat ]];
        then
		    
				 batchprocessing=${line:3:8}
				 sed -i 's/\r$//' $line
				 sed -e "s/$/$batchprocessing/" -i $line
				 gzip $line
         hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_PATH_MIDTPNR}
		 else
		        if [[ $line = pnrb*.txt ]];
        then
		    
				 batchprocessing=${line:3:8}
				  sed -i 's/\r$//' $line
				 sed -e "s/$/$batchprocessing/" -i $line
				 gzip $line
         hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_PATH_PNR_CNTRL}
		 else
		   if [[ $line = pnragyb*.txt ]];
        then
		     
				 batchprocessing=${line:6:8}
				  sed -i 's/\r$//' $line
				 sed -e "s/$/$batchprocessing/" -i $line
				 gzip $line
         hdfs dfs -copyFromLocal ${LOCAL_DIR}/$line.gz ${HDFS_PATH_AGENT_CNTRL}
           else
		                 echo "junk file"
                  fi
				  fi
				  fi
				  fi

done

#archiving files in FTP
######################################

cat $WRK_DIR/ftp_files_incremental.txt | while read line;
do
sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WRK_DIR/ftp_log
rename $FTP_DIR/$line $FTP_DIR/processed/"$line"_"$execution_date"
quit
END_SCRIPT
done

sftp -o StrictHostKeyChecking=no -o IdentityFile=$FTP_PUBLIC_KEY $FTP_USER@$FTP_HOST  <<END_SCRIPT > $WRK_DIR/ftp_log
rename $FTP_DIR/midt"$current_batch_date"_success.txt $FTP_DIR/processed/midt"$current_batch_date"_success.txt_"$execution_date"
quit
END_SCRIPT

	hdfs dfs -touchz ${HDFS_PATH_MIDTPNR}/_SUCCESS
	hdfs dfs -touchz ${HDFS_PATH_AGENT_MSTR}/_SUCCESS
	hdfs dfs -touchz ${HDFS_PATH_PNR_CNTRL}/_SUCCESS
	hdfs dfs -touchz ${HDFS_PATH_AGENT_CNTRL}/_SUCCESS


exit 0
