#! /bin/ksh
#-------------------------------------------------------------------------------
# Script name : ek_opseff_ags_hdfs_raw_import.sh                          
# Author      : Ravindra Chellubani                
# Created on  : 13-09-2017
# Modified on : 13-09-2017
# Modified by : Ravindra Chellubani
# Description : This script downloads AGS related files to Helix datalake on daily 
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
#Run cmds
#case 1: Runnning with Default params
#sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_hdfs_raw_import.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f <rv|se|n1|actual|monthly>
#case 2: To Manually process single file
#sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_hdfs_raw_import.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f <rv|se|n1|actual> -f <rv|se|n1|actual|montly> -n <filename>
#--------------------------------------------------------------------------------

#CASE 1 SAMPLE RUN CMD
#sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_hdfs_raw_import.sh  -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f rv

#MAIN EXECUTION BEGINS
echo "------------------------------------------------------------------"
echo "[INFO] Starting ek_opsbi_ags_hdfs_import script"
echo "------------------------------------------------------------------"

#FUNCTION TO CHECKING IF PARAMETERS ARE SPECIFIED CORRECTLY OR NOT
function cmd_line_argument_error()
{
	echo "[ERROR] Please make sure that no. of arguments are passed correctly"
	echo "[INFO] If running with default params : sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_hdfs_raw_import.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f <rv|se|n1|actual>"
	echo "[INFO] To manually process single file: sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_hdfs_raw_import.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env.env -f <rv|se|n1|actual> -f <rv|se|n1|actual> -n <filename>"
	exit 1;

}

#VALIDATING CMD ARGUMENTS
echo "[INFO] Validating Command Line Arguments"
_No_of_Args=`echo $#`
echo "[INFO] No.of arguments passed is : $_No_of_Args"

if [ $# -lt 4 ]||[ $# -gt 6 ]
then
cmd_line_argument_error
fi

#PARSING OF COMMAND LINE ARGUMENTS
OPTIND=1 # Sets intial value

while getopts p:f:n:m: OPTNS
do
        case $OPTNS in
        p) _Param_File=${OPTARG};;
		f) _Feed_Name=${OPTARG};;
		n) _File_Name=${OPTARG};;
        *) cmd_line_argument_error;;
        esac
done

#TRIMING EXTRA SPACES
_Param_File=`echo ${_Param_File} | tr -d " "`
_Feed_Name=`echo ${_Feed_Name} | tr -d " "`
_File_Name=`echo ${_File_Name} | tr -d " "`
_All_Files=`echo ${_All_Files} | tr -d " "`

#INCLUDING THE PARAMETER FILES AND COMMON LIBRARY 
. ${_Param_File}

#LOG FILE CREATION
echo "[INFO] Creating AGS log file...."
_var_Date=`date +%d%m%y%OH%OM%OS`
_var_Date_Only=`date +%Y-%m-%d`
_var_Time_Only=`date +%OH-%OM`
_Script_Full_Path=`echo "$0"` 														# This gives the fully qualified path
_Script_Name_Only=`echo "${_Script_Full_Path}" | awk -F "/" '{print $NF}'`			# Derived only script name from fully qualified path
_var_Log_File=`echo "${_Script_Name_Only}" | cut -d "." -f 1`						# Derived Log File Name from script name
if [ ! -d "${AGS_LOG_FILE_PATH}/$_Feed_Name" ]
	then
		mkdir -p ${AGS_LOG_FILE_PATH}/$_Feed_Name
		echo "[INFO] Created log directory ${AGS_LOG_FILE_PATH}/$_Feed_Name" | tee -a ${LOG_FILE}
else
		echo "[INFO] Log directory exists ${AGS_LOG_FILE_PATH}/$_Feed_Name" | tee -a ${LOG_FILE}
fi
LOG_FILE=${AGS_LOG_FILE_PATH}/$_Feed_Name/${_var_Log_File}_${_var_Date}.log
echo "[INFO] AGS log file created successfully : $LOG_FILE"
touch ${LOG_FILE}

#GETTING PROCESS ID        
_Process_ID=`echo $$`
echo "[INFO] Process ID : $_Process_ID" | tee -a ${LOG_FILE}
echo "[INFO] Passed Paramaters : PRAMA_FILE : $_Param_File, FEED_NAME : $_Feed_Name, FILE_NAME: $_File_Name" | tee -a ${LOG_FILE}

#CHECK FILES TO PROCESS
_file_cnt=`find ${AGS_OUTGOING_FILE_PATH}/$_Feed_Name/ -type f -name \*.* | wc -l`
_hdfs_cnt=0
if [ $_file_cnt -eq 0 ]
	then
		echo "[INFO] Zero files to Place from local directory ${AGS_OUTGOING_FILE_PATH}/$_Feed_Name/.. to HDFS RAW Hence exiting the process" | tee -a ${LOG_FILE}
		exit 0
fi

#FUNCTION TO SET HDFS FEED DIRECTORIES
function sethdfsddir(){
	echo "[INFO] Identifying $_Feed_Name HDFS directries" | tee -a ${LOG_FILE}
	if [ $_Feed_Name == "${AGS_FEED_RV}" ]
		then
			_Hdfs_Raw_File_Path=${AGS_HDFS_RAW_FILE_PATH}/${SOURCE_NAME}/${AGS_FEED_RV}/${LOAD_TYPE}/${VERSION}
			_Hdfs_Ctrl_File_Path=${AGS_HDFS_CTRL_FILE_PATH}/${SOURCE_NAME}/${AGS_FEED_RV}/${LOAD_TYPE}/${VERSION}
			_Stfp_File_Path=${AGS_SFTP_RV_FILE_PATH}
	elif [ $_Feed_Name == "${AGS_FEED_SE}" ]
		then
			_Hdfs_Raw_File_Path=${AGS_HDFS_RAW_FILE_PATH}/${SOURCE_NAME}/${AGS_FEED_SE}/${LOAD_TYPE}/${VERSION}
			_Hdfs_Ctrl_File_Path=${AGS_HDFS_CTRL_FILE_PATH}/${SOURCE_NAME}/${AGS_FEED_SE}/${LOAD_TYPE}/${VERSION}
			_Stfp_File_Path=${AGS_SFTP_SE_FILE_PATH}
	elif [ $_Feed_Name == "${AGS_FEED_N1}" ]
		then
			_Hdfs_Raw_File_Path=${AGS_HDFS_RAW_FILE_PATH}/${SOURCE_NAME}/${AGS_FEED_N1}/${LOAD_TYPE}/${VERSION}
			_Hdfs_Ctrl_File_Path=${AGS_HDFS_CTRL_FILE_PATH}/${SOURCE_NAME}/${AGS_FEED_N1}/${LOAD_TYPE}/${VERSION}
			_Stfp_File_Path=${AGS_SFTP_N1_FILE_PATH}
	elif [ $_Feed_Name == "${AGS_FEED_ACTUAL}" ]
		then
			_Hdfs_Raw_File_Path=${AGS_HDFS_RAW_FILE_PATH}/${SOURCE_NAME}/${AGS_FEED_ACTUAL}/${LOAD_TYPE}/${VERSION}
		    _Hdfs_Ctrl_File_Path=${AGS_HDFS_CTRL_FILE_PATH}/${SOURCE_NAME}/${AGS_FEED_ACTUAL}/${LOAD_TYPE}/${VERSION}
			_Stfp_File_Path=${AGS_SFTP_ACTUAL_FILE_PATH}
	elif [ $_Feed_Name == "${AGS_FEED_MONTHLY}" ]
		then
			_Hdfs_Raw_File_Path=${AGS_HDFS_RAW_FILE_PATH}/${SOURCE_NAME}/${AGS_FEED_MONTHLY}/${LOAD_TYPE}/${VERSION}
		    _Hdfs_Ctrl_File_Path=${AGS_HDFS_CTRL_FILE_PATH}/${SOURCE_NAME}/${AGS_FEED_MONTHLY}}/${LOAD_TYPE}/${VERSION}
			_Stfp_File_Path=${AGS_SFTP_MONTHLY_FILE_PATH}
	else
			echo "[ERROR] Unknown feed name. please check the feed name <rv|se|n1|actual|monthly>"
			exit 1;
	fi
	
	echo "[INFO] Successfully identified the HDFS directries for the feed $_Feed_Name. HDFS RAW PATH : $_Hdfs_Raw_File_Path, HDFS CRTL PATH : $_Hdfs_Ctrl_File_Path" | tee -a ${LOG_FILE}

}


#FUNCTION TO CREATE HDFS DIRECTORIES
function crthdfsdir(){
	echo "[INFO] Creating HDFS directries for the feed $_Feed_Name" | tee -a ${LOG_FILE}
	
	for file in "${AGS_INCOMING_FILE_PATH}/$_Feed_Name/"* ; 
	do
		filename=$(echo `basename $file` | cut -f 1 -d '.')
		echo "[INFO] Check whether following HDFS directries exists or not" | tee -a ${LOG_FILE}
		
		hadoop fs -ls $_Hdfs_Raw_File_Path/$_var_Date_Only/$_var_Time_Only/$filename
		_status=$?

		if [ $_status -eq 0 ]
			then
				echo "[INFO] HDFS RAW PATH $_Hdfs_Raw_File_Path/$_var_Date_Only/$filename exists"  | tee -a ${LOG_FILE}
				
		else	
				echo "[INFO] HDFS RAW PATH $_Hdfs_Raw_File_Path/$_var_Date_Only/$filename doesnt exist Hence creating the path" | tee -a ${LOG_FILE}
				hadoop fs -mkdir -p $_Hdfs_Raw_File_Path/$_var_Date_Only/$_var_Time_Only/$filename
				_status=$?
				if [ $_status -eq 0 ]
					then
						echo "[INFO] Creating HDFS RAW PATH $_Hdfs_Raw_File_Path/$_var_Date_Only/$_var_Time_Only/$filename were successfully processed" | tee -a ${LOG_FILE}

				else	
						echo "[ERROR] Creating HDFS RAW PATH $_Hdfs_Raw_File_Path/$_var_Date_Only/$_var_Time_Only/$filename is Unsuccessfull" | tee -a ${LOG_FILE}
						exit 1
				fi
		fi
				
	done
}

#FUNCTION TO PUT FILES HDFS DIRECTORIES
function localtohdfs(){

	echo "[INFO] Placing files from local directory ${AGS_OUTGOING_FILE_PATH}/$_Feed_Name/.. to $_Hdfs_Raw_File_Path/$_var_Date_Only/$_var_Time_Only.." | tee -a ${LOG_FILE}
	echo "[INFO] $_file_cnt files to Place from local directory ${AGS_OUTGOING_FILE_PATH}/$_Feed_Name/.. to $_Hdfs_Raw_File_Path/$_var_Date_Only/$_var_Time_Only.." | tee -a ${LOG_FILE}
	_file_list=`find ${AGS_OUTGOING_FILE_PATH}/$_Feed_Name/ -type f -name \*.${FILE_TYPE}`
	
	for file1 in "${AGS_INCOMING_FILE_PATH}/$_Feed_Name/"* ; 
	do
		_filename=$(echo `basename $file1` | cut -f 1 -d '.')
		_filter_file_list=`find ${AGS_OUTGOING_FILE_PATH}/$_Feed_Name/ -type f -name \*.${FILE_TYPE} | grep $_filename`
		#gzip files before moving to hdfs
		for file2 in $_filter_file_list;
		do
			echo "[INFO] Converting file $file2 to $_file2.gz format "| tee -a ${LOG_FILE}
			gzip $file2
			_status=$?
			if [ $_status -eq 0 ]
			then
				echo "[INFO] GZipping the file $file2 were successfully processed" | tee -a ${LOG_FILE}

			else	
				echo "[ERROR] GZipping the file $file2 is Unsuccessfull" | tee -a ${LOG_FILE}
				exit 1
			fi
			
			# moving gzfile to hdfs
			hdfs dfs -copyFromLocal -f $file2.gz $_Hdfs_Raw_File_Path/$_var_Date_Only/$_var_Time_Only/$_filename
			_status=$?
			if [ $_status -eq 0 ]
			then
				echo "[INFO] Placing the file $file2.gz to hdfs path $_Hdfs_Raw_File_Path/$_var_Date_Only/$_var_Time_Only/$_filename were successfully processed" | tee -a ${LOG_FILE}
				_hdfs_cnt=$(($_hdfs_cnt+1))
			else	
				echo "[ERROR] Placing the file $file2.gz to hdfs path $_Hdfs_Raw_File_Path/$_var_Date_Only/$_var_Time_Only/$_filename is Unsuccessfull" | tee -a ${LOG_FILE}
				exit 1
			fi
			
		done
	done
	
}



#CALLING SETFEEDDIR
sethdfsddir
#CALLING CREATE HDFS DIRECTORIES
crthdfsdir
#CALLING LOCAL TO HDFS DIRECTORIES
localtohdfs

#UNIT TEST
if [ $_file_cnt -eq $_hdfs_cnt ]
	then
		echo "[INFO] Unit Test successfully passed. Local file count : $_file_cnt, HDFS file count : $_hdfs_cnt" | tee -a ${LOG_FILE}
else
		echo "[INFO] Unit Test failed. Local file count : $_file_cnt, HDFS file count : $_hdfs_cnt" | tee -a ${LOG_FILE}
		exit 1

fi

echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}
echo "[INFO] Ending ek_opsbi_ags_hdfs_import script successfully" | tee -a ${LOG_FILE}
echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}	
		
exit 0;