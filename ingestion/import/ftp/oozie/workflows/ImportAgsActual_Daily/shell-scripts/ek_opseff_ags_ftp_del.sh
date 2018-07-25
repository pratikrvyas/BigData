#! /bin/ksh
#-------------------------------------------------------------------------------
# Script name : ek_opseff_ags_ftp_del.sh                           
# Author      : Ravindra Chellubani                
# Created on  : 13-09-2017
# Modified on : 13-09-2017
# Modified by : Ravindra Chellubani
# Description : This script deletes AGS related files from FTP 
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
#Run cmds
#case 1: Runnning with Default params
#sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_ftp_del.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f <rv|se|n1|actual|monthly>
#case 2: To Manually process single file
#sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_ftp_del.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f <rv|se|n1|actual> -n <filename>
#--------------------------------------------------------------------------------

#CASE 1 SAMPLE RUN CMD
#sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_ftp_del.sh  -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f rv

#MAIN EXECUTION BEGINS
echo "------------------------------------------------------------------"
echo "[INFO] Starting ek_opseff_ags_ftp_del.sh script"
echo "------------------------------------------------------------------"

#FUNCTION TO CHECKING IF PARAMETERS ARE SPECIFIED CORRECTLY OR NOT
function cmd_line_argument_error()
{
	echo "[ERROR] Please make sure that no. of arguments are passed correctly"
	echo "[INFO] If running with default params : sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_ftp_del.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f <rv|se|n1|actual>"
	echo "[INFO] To manually process single file: sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_ftp_del.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f <rv|se|n1|actual> -f <rv|se|n1|actual> -n <filename>"
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
_var_Date_Only=`date +%d-%m-%Y`
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

echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}
echo "[INFO] Script Name : ek_opseff_ags_ftp_del.sh script" | tee -a ${LOG_FILE}
echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}

#GETTING PROCESS ID        
_Process_ID=`echo $$`
echo "[INFO] Process ID : $_Process_ID" | tee -a ${LOG_FILE}
echo "[INFO] Passed Paramaters : PRAMA_FILE : $_Param_File, FEED_NAME : $_Feed_Name, FILE_NAME: $_File_Name" | tee -a ${LOG_FILE}

#FUNCTION TO SET HDFS FEED DIRECTORIES
function setstatusdir(){
	echo "[INFO] Identifying $_Feed_Name FTP and STATUS directries" | tee -a ${LOG_FILE}
	if [ $_Feed_Name == "${AGS_FEED_RV}" ]
		then
			_Sftp_File_Path=${AGS_SFTP_RV_FILE_PATH}
			_Status_File_Path=${AGS_STATUS_FILE_PATH}/$_Feed_Name
	elif [ $_Feed_Name == "${AGS_FEED_SE}" ]
		then
			_Sftp_File_Path=${AGS_SFTP_SE_FILE_PATH}
			_Status_File_Path=${AGS_STATUS_FILE_PATH}/$_Feed_Name
	elif [ $_Feed_Name == "${AGS_FEED_N1}" ]
		then
			_Sftp_File_Path=${AGS_SFTP_N1_FILE_PATH}
			_Status_File_Path=${AGS_STATUS_FILE_PATH}/$_Feed_Name
	elif [ $_Feed_Name == "${AGS_FEED_ACTUAL}" ]
		then
			_Sftp_File_Path=${AGS_SFTP_ACTUAL_FILE_PATH}
			_Status_File_Path=${AGS_STATUS_FILE_PATH}/$_Feed_Name
	elif [ $_Feed_Name == "${AGS_FEED_MONTHLY}" ]
		then
			_Sftp_File_Path=${AGS_SFTP_MONTHLY_FILE_PATH}
			_Status_File_Path=${AGS_STATUS_FILE_PATH}/$_Feed_Name
	else
			echo "[ERROR] Unknown feed name. please check the feed name <rv|se|n1|actual|monthly>"
			exit 1;
	fi
	
	echo "[INFO] Successfully identified the FTP & STATUS directries for the feed $_Feed_Name. FTP PATH : $_Sftp_File_Path STATUS PATH : $_Status_File_Path" | tee -a ${LOG_FILE}

}

function ftpbatchcmdcrt(){
	
	#Creating the cmds to list and delete files from ftp
	echo "[INFO] Creating ${AGS_PARAM_FILE_PATH}/ek_opseff_ags_ftp_batchcmd.cmd file..." | tee -a ${LOG_FILE}
	
	rm  ${AGS_PARAM_FILE_PATH}/${_Feed_Name}_ls_${SFTP_BATCH_FILE}
	rm  ${AGS_PARAM_FILE_PATH}/${_Feed_Name}_rm_${SFTP_BATCH_FILE}
	_status=$?
	if [ $_status -ne 0 ]
	then
		echo "[INFO] Cannot delete ${AGS_PARAM_FILE_PATH}/ek_opseff_ags_ftp_batchcmd.cmd file" | tee -a ${LOG_FILE}
	fi

	touch ${AGS_PARAM_FILE_PATH}/${_Feed_Name}_ls_${SFTP_BATCH_FILE}
	touch ${AGS_PARAM_FILE_PATH}/${_Feed_Name}_rm_${SFTP_BATCH_FILE}
	_status=$?
	if [ $_status -ne 0 ]
	then
		echo "[ERROR] Cannot create ${AGS_PARAM_FILE_PATH}/ek_opseff_ags_ftp_batchcmd.cmd file" | tee -a ${LOG_FILE}
		exit 1
	fi
	
	echo "ls ${_Sftp_File_Path}/*" | tee -a ${AGS_PARAM_FILE_PATH}/${_Feed_Name}_ls_${SFTP_BATCH_FILE}
	echo "rm ${_Sftp_File_Path}/*" | tee -a ${AGS_PARAM_FILE_PATH}/${_Feed_Name}_rm_${SFTP_BATCH_FILE}
	
	echo "[INFO] Created ${AGS_PARAM_FILE_PATH}/ek_opseff_ags_ftp_batchcmd.cmd file successfully" | tee -a ${LOG_FILE}
	
}

function ftpfilesdel(){
	 
	#Listing files to be deleted form FTP
	echo "[INFO] Listing files to be deleted form FTP ${AGS_SFTP_USER}@${AGS_SFTP_HOSTNAME} location $_Sftp_File_Path" | tee -a ${LOG_FILE}
	sftp -b ${AGS_PARAM_FILE_PATH}/${_Feed_Name}_ls_${SFTP_BATCH_FILE} ${AGS_SFTP_USER}@${AGS_SFTP_HOSTNAME} | tee -a ${LOG_FILE}
	_count=`sftp -b ${AGS_PARAM_FILE_PATH}/${_Feed_Name}_ls_${SFTP_BATCH_FILE} ${AGS_SFTP_USER}@${AGS_SFTP_HOSTNAME} | wc -l`
	if [ $_count -ne 1 ]
	then
		#removing files from FTP location to process new files
		sftp -b ${AGS_PARAM_FILE_PATH}/${_Feed_Name}_rm_${SFTP_BATCH_FILE} ${AGS_SFTP_USER}@${AGS_SFTP_HOSTNAME}
		_status=$?
		if [ $_status -eq 0 ]
		then
			echo "[INFO] Removing the files from FTP ${AGS_SFTP_USER}@${AGS_SFTP_HOSTNAME} location $_Sftp_File_Path were successfully processed" | tee -a ${LOG_FILE}
			if [ ! -d "$_Status_File_Path" ]
				then
					mkdir -p $_Status_File_Path
					touch "$_Status_File_Path/${AGS_STATUS_FILE_NAME}"
					echo "[INFO] Creating trigger file $_Status_File_Path/${AGS_STATUS_FILE_NAME} Sucessfully processed" | tee -a ${LOG_FILE}
			else
					touch "$_Status_File_Path/${AGS_STATUS_FILE_NAME}"
					echo "[INFO] Creating trigger file $_Status_File_Path/${AGS_STATUS_FILE_NAME} Sucessfully processed" | tee -a ${LOG_FILE}
			fi
		else	
			echo "[ERROR] Removing the files from FTP ${AGS_SFTP_USER}@${AGS_SFTP_HOSTNAME} location $_Sftp_File_Path is Unsuccessfull" | tee -a ${LOG_FILE}
			exit 1
		fi
	else
		if [ ! -d "$_Status_File_Path" ]
			then
				mkdir -p $_Status_File_Path
				touch "$_Status_File_Path/${AGS_STATUS_FILE_NAME}"
				echo "[INFO] Creating trigger file $_Status_File_Path/${AGS_STATUS_FILE_NAME} Sucessfully processed" | tee -a ${LOG_FILE}
		else
				touch "$_Status_File_Path/${AGS_STATUS_FILE_NAME}"
				echo "[INFO] Creating trigger file $_Status_File_Path/${AGS_STATUS_FILE_NAME} Sucessfully processed" | tee -a ${LOG_FILE}
		fi
		echo "[INFO] Zero files to delete from FTP ${AGS_SFTP_USER}@${AGS_SFTP_HOSTNAME} location $_Sftp_File_Path. Hence graceful exit" | tee -a ${LOG_FILE}
	fi	
}

#CALLING SETSTATUSDIRS
setstatusdir
#CALLING CREATING BATCHCMD FILE
ftpbatchcmdcrt
#CALLING FTPFILESDEL
ftpfilesdel

echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}
echo "[INFO] Ending ek_opseff_ags_ftp_del.sh script successfully" | tee -a ${LOG_FILE}
echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}

exit 0
