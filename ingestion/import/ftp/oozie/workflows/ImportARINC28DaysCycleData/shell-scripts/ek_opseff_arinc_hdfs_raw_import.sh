#! /bin/ksh
#-------------------------------------------------------------------------------
# Script name : ek_opseff_arinc_hdfs_raw_import.sh                         
# Author      : Ravindra Chellubani                
# Created on  : 25-10-2017
# Modified on : 
# Modified by : Ravindra Chellubani
# Description : This script downloads ARINC related files to Helix datalake 
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
#Run cmds
#case 1: Runnning with Default params
#sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_hdfs_raw_import.sh -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env -i <HDFS_RAW_PATH>
#--------------------------------------------------------------------------------

#CASE 1 SAMPLE RUN CMD
#sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_hdfs_raw_import.sh  -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env -i /data/helix/raw/arinc/incremental/1.0/28-10-2017/12-00/

#MAIN EXECUTION BEGINS
echo "------------------------------------------------------------------"
echo "[INFO] Starting ek_opsbi_arinc_hdfs_import script"
echo "------------------------------------------------------------------"

#FUNCTION TO CHECKING IF PARAMETERS ARE SPECIFIED CORRECTLY OR NOT
function cmd_line_argument_error()
{
	echo "[ERROR] Please make sure that no. of arguments are passed correctly"
	echo "[INFO] If running with default params : sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_hdfs_raw_import.sh -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env -i <HDFS_RAW_PATH>"
	exit 1;

}

#VALIDATING CMD ARGUMENTS
echo "[INFO] Validating Command Line Arguments"
_No_of_Args=`echo $#`
echo "[INFO] No.of arguments passed is : $_No_of_Args"

if [ $# -ne 4 ]
then
cmd_line_argument_error
fi

#PARSING OF COMMAND LINE ARGUMENTS
OPTIND=1 # Sets intial value

while getopts p:i: OPTNS
do
	case $OPTNS in
	p) _Param_File=${OPTARG};;
	i) _HDFS_Path=${OPTARG};;
	*) cmd_line_argument_error;;
	esac
done

#TRIMING EXTRA SPACES
_Param_File=`echo ${_Param_File} | tr -d " "`
_HDFS_Path=`echo ${_HDFS_Path} | tr -d " "`

#INCLUDING THE PARAMETER FILES AND COMMON LIBRARY 
. ${_Param_File}

#LOG FILE CREATION
echo "[INFO] Creating ARINC log file...."
_var_Date=`date +%d%m%y%OH%OM%OS`
_var_Date_Only=`date +%Y-%m-%d`
_var_Time_Only=`date +%OH-%OM`
_Script_Full_Path=`echo "$0"` 														# This gives the fully qualified path
_Script_Name_Only=`echo "${_Script_Full_Path}" | awk -F "/" '{print $NF}'`			# Derived only script name from fully qualified path
_var_Log_File=`echo "${_Script_Name_Only}" | cut -d "." -f 1`						# Derived Log File Name from script name
if [ ! -d "${LOG_FILE_PATH}" ]
	then
		mkdir -p ${LOG_FILE_PATH}
		echo "[INFO] Created log directory ${LOG_FILE_PATH}" | tee -a ${LOG_FILE}
else
		echo "[INFO] Log directory exists ${LOG_FILE_PATH}" | tee -a ${LOG_FILE}
fi
LOG_FILE=${LOG_FILE_PATH}/${_var_Log_File}_${_var_Date}.log
echo "[INFO] ARINC log file created successfully : $LOG_FILE"
touch ${LOG_FILE}

#GETTING PROCESS ID        
_Process_ID=`echo $$`
echo "[INFO] Process ID : $_Process_ID" | tee -a ${LOG_FILE}
echo "[INFO] Passed Paramaters : PRAMA_FILE : $_Param_File, HDFS_RAW_PATH : $_HDFS_Path" | tee -a ${LOG_FILE}

#CHECK FILES TO PROCESS
_file_cnt=`find ${INCOMING_FILE_PATH} -type f -name \*.* | wc -l`
_hdfs_cnt=0
if [ $_file_cnt -eq 0 ]
	then
		echo "[INFO] Zero files to Place from local directory ${INCOMING_FILE_PATH}/.. to HDFS RAW Hence exiting the process" | tee -a ${LOG_FILE}
		exit 0
fi

#FUNCTION TO SET HDFS FEED DIRECTORIES
function sethdfsddir(){
	echo "[INFO] Identifying HDFS directries" | tee -a ${LOG_FILE}
	_Hdfs_Raw_File_Path=$_HDFS_Path
	echo "[INFO] Successfully identified the HDFS directries. HDFS RAW PATH : $_Hdfs_Raw_File_Path" | tee -a ${LOG_FILE}
}


#FUNCTION TO CREATE HDFS DIRECTORIES
function crthdfsdir(){

	echo "[INFO] Check whether following HDFS directries exists or not" | tee -a ${LOG_FILE}
		
	hadoop fs -ls $_Hdfs_Raw_File_Path
	_status=$?

	if [ $_status -eq 0 ]
		then
			echo "[INFO] HDFS RAW PATH $_Hdfs_Raw_File_Path"  | tee -a ${LOG_FILE}
			
	else	
			echo "[INFO] HDFS RAW PATH $_Hdfs_Raw_File_Path doesnt exist Hence creating the path" | tee -a ${LOG_FILE}
			hadoop fs -mkdir -p $_Hdfs_Raw_File_Path
			_status=$?
			if [ $_status -eq 0 ]
				then
					echo "[INFO] Creating HDFS RAW PATH $_Hdfs_Raw_File_Path were successfully processed" | tee -a ${LOG_FILE}

			else	
					echo "[ERROR] Creating HDFS RAW PATH $_Hdfs_Raw_File_Path is Unsuccessfull" | tee -a ${LOG_FILE}
					exit 1
			fi
	fi

}

#FUNCTION TO PUT FILES HDFS DIRECTORIES
function localtohdfs(){

	echo "[INFO] Placing files from local directory ${INCOMING_FILE_PATH}.. to $_Hdfs_Raw_File_Path.." | tee -a ${LOG_FILE}
	
	echo "[INFO] $_file_cnt files to Place from local directory ${INCOMING_FILE_PATH}.. to $_Hdfs_Raw_File_Path.." | tee -a ${LOG_FILE}
	
	for file1 in "${INCOMING_FILE_PATH}/"* ; 
	do
		echo $file1
		# moving gzfile to hdfs
		hdfs dfs -copyFromLocal -f $file1 $_Hdfs_Raw_File_Path
		_status=$?
		if [ $_status -eq 0 ]
		then
			echo "[INFO] Placing the file $file1 to hdfs path $_Hdfs_Raw_File_Path were successfully processed" | tee -a ${LOG_FILE}
			_hdfs_cnt=$(($_hdfs_cnt+1))
		else	
			echo "[ERROR] Placing the file $file1 to hdfs path $_Hdfs_Raw_File_Path is Unsuccessfull" | tee -a ${LOG_FILE}
			exit 1
		fi
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
echo "[INFO] Ending ek_opsbi_arinc_hdfs_import script successfully" | tee -a ${LOG_FILE}
echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}	
		
exit 0;