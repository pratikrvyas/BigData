#! /bin/ksh
#-------------------------------------------------------------------------------
# Script name : ek_opseff_arinc_ftp_del.sh                          
# Author      : Ravindra Chellubani                
# Created on  : 
# Modified on :
# Modified by : 
# Description : This script deletes ARINC related files from FTP 
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
#Run cmds
#case 1: Runnning with Default params
#sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_ftp_del.sh -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env
#--------------------------------------------------------------------------------

#CASE 1 SAMPLE RUN CMD
#sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_ftp_del.sh  -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env

#MAIN EXECUTION BEGINS
echo "------------------------------------------------------------------"
echo "[INFO] Starting ek_opseff_arinc_ftp_del.sh script"
echo "------------------------------------------------------------------"

#FUNCTION TO CHECKING IF PARAMETERS ARE SPECIFIED CORRECTLY OR NOT
function cmd_line_argument_error()
{
	echo "[ERROR] Please make sure that no. of arguments are passed correctly"
	echo "[INFO] If running with default params : sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_ftp_del.sh -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env"
	exit 1;
}

#VALIDATING CMD ARGUMENTS
echo "[INFO] Validating Command Line Arguments"
_No_of_Args=`echo $#`
echo "[INFO] No.of arguments passed is : $_No_of_Args"

if [ $# -ne 2 ]
then
cmd_line_argument_error
fi

#PARSING OF COMMAND LINE ARGUMENTS
OPTIND=1 # Sets intial value

while getopts p: OPTNS
do
	case $OPTNS in
	p) _Param_File=${OPTARG};;
	*) cmd_line_argument_error;;
	esac
done

#TRIMING EXTRA SPACES
_Param_File=`echo ${_Param_File} | tr -d " "`

#INCLUDING THE PARAMETER FILES AND COMMON LIBRARY 
. ${_Param_File}

#LOG FILE CREATION
echo "[INFO] Creating ARINC log file...."
_var_Date=`date +%d%m%y%OH%OM%OS`
_var_Date_Only=`date +%d-%m-%Y`
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

echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}
echo "[INFO] Script Name : ek_opseff_arinc_ftp_del.sh script" | tee -a ${LOG_FILE}
echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}

#GETTING PROCESS ID        
_Process_ID=`echo $$`
echo "[INFO] Process ID : $_Process_ID" | tee -a ${LOG_FILE}
echo "[INFO] Passed Paramaters : PRAMA_FILE : $_Param_File" | tee -a ${LOG_FILE}

#FUNCTION TO SET HDFS FEED DIRECTORIES
function setstatusdir(){
	echo "[INFO] Identifying $_Feed_Name FTP and STATUS directries" | tee -a ${LOG_FILE}
	_Sftp_File_Path=${SFTP_ARINC_FILE_PATH}
	_Status_File_Path=${STATUS_FILE_PATH}	
	echo "[INFO] Successfully identified the FTP & STATUS directries. FTP PATH : $_Sftp_File_Path STATUS PATH : $_Status_File_Path" | tee -a ${LOG_FILE}

}

function ftpbatchcmdcrt(){
	
	#Creating the cmds to list and delete files from ftp
	echo "[INFO] Creating ${PARAM_FILE_PATH}/ek_opseff_arinc_ftp_batchcmd.cmd file..." | tee -a ${LOG_FILE}
	
	rm  ${PARAM_FILE_PATH}/_ls_${SFTP_BATCH_FILE}
	rm  ${PARAM_FILE_PATH}/_rm_${SFTP_BATCH_FILE}

	touch ${PARAM_FILE_PATH}/_ls_${SFTP_BATCH_FILE}
	touch ${PARAM_FILE_PATH}/_rm_${SFTP_BATCH_FILE}
	_status=$?
	if [ $_status -ne 0 ]
	then
		echo "[ERROR] Cannot create ${PARAM_FILE_PATH}/_rm_${SFTP_BATCH_FILE} file" | tee -a ${LOG_FILE}
		exit 1
	fi
	
	echo "ls ${_Sftp_File_Path}/*" | tee -a ${PARAM_FILE_PATH}/_ls_${SFTP_BATCH_FILE}
	echo "rm ${_Sftp_File_Path}/*" | tee -a ${PARAM_FILE_PATH}/_rm_${SFTP_BATCH_FILE}
	
	echo "[INFO] Created ${PARAM_FILE_PATH}/_rm_${SFTP_BATCH_FILE} file successfully" | tee -a ${LOG_FILE}
	
}

function ftpfilesdel(){
	 
	#Listing files to be deleted form FTP
	echo "[INFO] Listing files to be deleted form FTP ${SFTP_USER}@${SFTP_HOSTNAME} location $_Sftp_File_Path" | tee -a ${LOG_FILE}
	sftp -b ${PARAM_FILE_PATH}/_ls_${SFTP_BATCH_FILE} ${SFTP_USER}@${SFTP_HOSTNAME} | tee -a ${LOG_FILE}
	_count=`sftp -b ${PARAM_FILE_PATH}/_ls_${SFTP_BATCH_FILE} ${SFTP_USER}@${SFTP_HOSTNAME} | wc -l`
	if [ $_count -ne 1 ]
	then
		#removing files from FTP location to process new files
		sftp -b ${PARAM_FILE_PATH}/_rm_${SFTP_BATCH_FILE} ${SFTP_USER}@${SFTP_HOSTNAME}
		_status=$?
		if [ $_status -eq 0 ]
		then
			echo "[INFO] Removing the files from FTP ${SFTP_USER}@${SFTP_HOSTNAME} location $_Sftp_File_Path were successfully processed" | tee -a ${LOG_FILE}
			if [ ! -d "$_Status_File_Path" ]
				then
					mkdir -p $_Status_File_Path
					touch "$_Status_File_Path/${STATUS_FILE_NAME}"
					echo "[INFO] Creating trigger file $_Status_File_Path/${STATUS_FILE_NAME} Sucessfully processed" | tee -a ${LOG_FILE}
			else
					touch "$_Status_File_Path/${STATUS_FILE_NAME}"
					echo "[INFO] Creating trigger file $_Status_File_Path/${STATUS_FILE_NAME} Sucessfully processed" | tee -a ${LOG_FILE}
			fi
		else	
			echo "[ERROR] Removing the files from FTP ${SFTP_USER}@${SFTP_HOSTNAME} location $_Sftp_File_Path is Unsuccessfull" | tee -a ${LOG_FILE}
			exit 1
		fi
	else
		if [ ! -d "$_Status_File_Path" ]
			then
				mkdir -p $_Status_File_Path
				touch "$_Status_File_Path/${STATUS_FILE_NAME}"
				echo "[INFO] Creating trigger file $_Status_File_Path/${STATUS_FILE_NAME} Sucessfully processed" | tee -a ${LOG_FILE}
		else
				touch "$_Status_File_Path/${STATUS_FILE_NAME}"
				echo "[INFO] Creating trigger file $_Status_File_Path/${STATUS_FILE_NAME} Sucessfully processed" | tee -a ${LOG_FILE}
		fi
		echo "[INFO] Zero files to delete from FTP ${SFTP_USER}@${SFTP_HOSTNAME} location $_Sftp_File_Path. Hence graceful exit" | tee -a ${LOG_FILE}
	fi	
}

#CALLING SETSTATUSDIRS
setstatusdir
#CALLING CREATING BATCHCMD FILE
ftpbatchcmdcrt
#CALLING FTPFILESDEL
ftpfilesdel

echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}
echo "[INFO] Ending ek_opseff_arinc_ftp_del.sh script successfully" | tee -a ${LOG_FILE}
echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}

exit 0
