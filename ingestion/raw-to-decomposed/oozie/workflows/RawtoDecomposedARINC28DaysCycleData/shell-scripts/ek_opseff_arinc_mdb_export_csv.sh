#! /bin/ksh
#-------------------------------------------------------------------------------------
# Script name : ek_opseff_arinc_mdb_export_csv.sh.                        
# Author      : Ravindra Chellubani,Pratik Vyas               
# Created on  : 30-10-2017
# Modified on : 
# Modified by : 
# Description : This script will export ARINC mdb to csv and moves to hdfs decomposed
#------------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
#Run cmds
#case 1: Runnning with Default params
#sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_mdb_export_csv.sh -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env -t <TABLE_NAME> -i <HDFS_TMP_PATH>
#--------------------------------------------------------------------------------

#CASE 1 SAMPLE RUN CMD
#sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_mdb_export_csv.sh  -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env -t Airport -i /data/helix/decomposed/arinc/tmp 

#MAIN EXECUTION BEGINS
echo "------------------------------------------------------------------"
echo "[INFO] Starting ek_opseff_arinc_mdb_export_csv script"
echo "------------------------------------------------------------------"

#FUNCTION TO CHECKING IF PARAMETERS ARE SPECIFIED CORRECTLY OR NOT
function cmd_line_argument_error()
{
	echo "[ERROR] Please make sure that no. of arguments are passed correctly"
	echo "[INFO] If running with default params : sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_mdb_export_csv.sh -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env -t <TABLE_NAME> -i <HDFS_TMP_PATH>"
	exit 1;

}

#VALIDATING CMD ARGUMENTS
echo "[INFO] Validating Command Line Arguments"
_No_of_Args=`echo $#`
echo "[INFO] No.of arguments passed is : $_No_of_Args"

if [ $# -ne 6 ]
then
cmd_line_argument_error
fi

#PARSING OF COMMAND LINE ARGUMENTS
OPTIND=1 # Sets intial value

while getopts p:t:i: OPTNS
do
	case $OPTNS in
	p) _Param_File=${OPTARG};;
	t) _MDB_Table_name=${OPTARG};;
	i) _HDFS_Tmp_Path=${OPTARG};;
	*) cmd_line_argument_error;;
	esac
done

#TRIMING EXTRA SPACES
_Param_File=`echo ${_Param_File} | tr -d " "`
_MDB_Table_name=`echo ${_MDB_Table_name} | tr -d " "`
_HDFS_Tmp_Path=`echo ${_HDFS_Tmp_Path} | tr -d " "`

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
echo "[INFO] Passed Paramaters : PRAMA_FILE : $_Param_File, MDB_TABLE_NAME : $_MDB_Table_name, HDFS_TMP_PATH : _HDFS_Tmp_Path" | tee -a ${LOG_FILE}

#CHECK FILES TO PROCESS
_file_cnt=`find ${OUTGOING_FILE_PATH} -type f -name \*.${FILE_TYPE_LOWERCASE} | wc -l`
if [ $_file_cnt -eq 0 ]||[ $_file_cnt -gt 1 ]
	then
		echo "[ERROR] Zero or Greater than 1 file to Process from outgoing directory ${OUTGOING_FILE_PATH}/.. Hence exiting the process" | tee -a ${LOG_FILE}
		exit 1
else
	echo "[INFO] 1 file found in outgoing directory ${OUTGOING_FILE_PATH}/.. proceeding with next steps.." | tee -a ${LOG_FILE}
fi

#FUNCTION TO EXTRACT MDB FILE
function extractmdbfile(){
	echo "[INFO] Extract table $_MDB_Table_name and saving to local directory ${OUTGOING_FILE_PATH} from mdb file ..."
	mdb-export -H ${OUTGOING_FILE_PATH}/*.${FILE_TYPE_LOWERCASE} $_MDB_Table_name > ${OUTGOING_FILE_PATH}/$_MDB_Table_name.csv
	_status=$?
	
	if [ $_status -eq 0 ]
		then
			echo "[INFO] Extracting table $_MDB_Table_name and saving to local directory ${OUTGOING_FILE_PATH} from mdb file is successful"  | tee -a ${LOG_FILE}
			
	else	
			echo "[ERROR] Extracting table $_MDB_Table_name and saving to local directory ${OUTGOING_FILE_PATH} from mdb file is unsuccessful" | tee -a ${LOG_FILE}
			exit 1
	fi
}


#FUNCTION TO GET FILES FROM HDFS RAW TO LOCAL
function puthdfsrawfile(){
	echo "[INFO] Putting files Local to HDFS for further processing..."
	
	hdfs dfs -mkdir -p $_HDFS_Tmp_Path/$_MDB_Table_name
	
	hdfs dfs -rm -r $_HDFS_Tmp_Path/$_MDB_Table_name/*
	
	hdfs dfs -put ${OUTGOING_FILE_PATH}/$_MDB_Table_name.csv $_HDFS_Tmp_Path/$_MDB_Table_name
	_status=$?
	
	if [ $_status -eq 0 ]
		then
			echo "[INFO] Putting file from Local to HDFS is successful"  | tee -a ${LOG_FILE}
			
	else	
			echo "[ERROR] Putting file from Local to HDFS is unsuccessful" | tee -a ${LOG_FILE}
			exit 1
	fi
}


#CALLING SETFEEDDIR
extractmdbfile
#CALLING CREATE HDFS DIRECTORIES
puthdfsrawfile

echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}
echo "[INFO] Ending ek_opseff_arinc_mdb_export_csv script successfully" | tee -a ${LOG_FILE}
echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}	
		
exit 0;