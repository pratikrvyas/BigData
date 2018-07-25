#-------------------------------------------------------------------------------------
# Script name : ek_opseff_arinc_ftp_import_daily.sh
# Author      : Ravindra Chellubani                
# Created on  : 27-10-2017
# Modified on : 
# Modified by : 
# Description : This script downloads ARINC related files to Helix datalake on MONTHLY 
#-------------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
#Run cmds
#case 1: Runnning with Default params
#sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_ftp_import_daily.sh -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env
#--------------------------------------------------------------------------------

#MAIN EXECUTION BEGINS
echo "------------------------------------------------------------------"
echo "[INFO] Starting ek_opsbi_arinc_ftp_import script"
echo "------------------------------------------------------------------"

#FUNCTION TO CHECKING IF PARAMETERS ARE SPECIFIED CORRECTLY OR NOT
function cmd_line_argument_error()
{
	echo "[ERROR] Please make sure that no. of arguments are passed correctly"
	echo "[INFO] If running with default params : sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_arinc_ftp_import_daily.sh -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env"
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
echo "[INFO] Creating ARINC log file successfully"
_var_Date=`date +%d%m%y%OH%OM%OS`
_var_Date_Only=`date +%d%m%Y`
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
echo "[INFO] Passed Paramaters : PRAMA_FILE : $_Param_File" | tee -a ${LOG_FILE}

#FUNCTION TO CHECK TRIGGER
function chkftpdeltrg(){

	echo "[INFO] Checking the success trigger file for proceeding further. This check avoids duplicates in raw layer" | tee -a ${LOG_FILE}
	
	if [ ! -f "${STATUS_FILE_PATH}/$_Feed_Name/${STATUS_FILE_NAME}" ]
	then
		echo "[ERROR] Previous run was unsuccessfull success trigger doesn't exist in ${STATUS_FILE_PATH}/$_Feed_Name/. please check " | tee -a ${LOG_FILE}
		exit 1
	else	
		echo "[INFO] Previous run was successfull success trigger exist in ${STATUS_FILE_PATH}/$_Feed_Name/. Proceeding with next steps.. " | tee -a ${LOG_FILE}
	fi	
	
}


#FUNCTION TO SET FEED DIRECTORIES
function setfeeddir(){

	echo "[INFO] Identifying directries...." | tee -a ${LOG_FILE}

	_FTP_File_Path=${SFTP_ARINC_FILE_PATH}
	_Incoming_File_Path=${INCOMING_FILE_PATH}
	_Archive_File_Path=${ARCHIVE_FILE_PATH}

	echo "[INFO] Successfully identified the feed directries. FTP PATH : $_FTP_File_Path, INCOMING PATH : $_Incoming_File_Path, ARCHIVE PATH : $_Archive_File_Path" | tee -a ${LOG_FILE}

}

#FUNCTION TO CLEAN DIRECTORIES
function archivedirs(){

echo "[INFO] Cheching if the archive directory exists or not. otherwise creates a archive directory : ${_Archive_File_Path}/${_var_Date_Only}" | tee -a ${LOG_FILE}

	if [ ! -d "${_Archive_File_Path}/${_var_Date_Only}" ]
		then
			mkdir -p ${_Archive_File_Path}/${_var_Date_Only}
			echo "[INFO] Created archive directory" | tee -a ${LOG_FILE}
	else
			echo "[INFO] Archive directory exists" | tee -a ${LOG_FILE}
	fi

	echo "[INFO] Archiving files in incoming directory ${_Incoming_File_Path} to archive directory ${_Archive_File_Path}/${_var_Date_Only}"  | tee -a ${LOG_FILE}
	mv -f ${_Incoming_File_Path}/* ${_Archive_File_Path}/${_var_Date_Only}/
	_status=$?
	if [ $_status -eq 0 ]
		then
			echo "[INFO] List of archived files: "$(ls -R ${_Archive_File_Path}/${_var_Date_Only}) | tee -a ${LOG_FILE}
			echo "[INFO] Archiving files in incoming directory ${_Incoming_File_Path} and moving to archive directory ${_Archive_File_Path}/${_var_Date_Only} were successfully processed" | tee -a ${LOG_FILE}

	else	
			echo "[ERROR] Archiving files in incoming directory ${_Incoming_File_Path} and moving to Archive directory ${_Archive_File_Path}/${_var_Date_Only} is Unsuccessfull" | tee -a ${LOG_FILE}
				
	fi
}

#FUNCTION TO GET FILE FROM SFTP
function sftpget(){

	echo "[INFO] Cheching if the incoming directory exists or not. otherwise creates a Incoming directory : ${_Incoming_File_Path}" | tee -a ${LOG_FILE}

	if [ ! -d "${_Incoming_File_Path}" ]
		then
			mkdir -p ${_Incoming_File_Path}
			echo "[INFO] Created incoming directory" | tee -a ${LOG_FILE}
	else
			echo "[INFO] Incoming directory exists" | tee -a ${LOG_FILE}
	fi

	echo "[INFO] Getting files from remote location $SFTP_USER@$SFTP_HOSTNAME:$_FTP_File_Path to incoming directory ${_Incoming_File_Path}"  | tee -a ${LOG_FILE}
	
	#FTP CMD TO GET FILES FROM THE REMOTE HOST
	sftp $SFTP_USER@$SFTP_HOSTNAME:$_FTP_File_Path/*.mdb ${_Incoming_File_Path}
	_status=$?
	if [ $_status -eq 0 ]
		then
			echo "[INFO] List of files: " | tee -a ${LOG_FILE}
			echo `ls ${_Incoming_File_Path}` | tee -a ${LOG_FILE}
			echo "[INFO] Getting files from remote location $SFTP_USER@$SFTP_HOSTNAME:$_FTP_File_Path to incoming directory ${_Incoming_File_Path} were successfully processed" | tee -a ${LOG_FILE}
	else
			if [ $_status -eq 1 ]
				then
					echo "[ERROR] Unknown remote file path $SFTP_USER@$SFTP_HOSTNAME:$_FTP_File_Path" | tee -a ${LOG_FILE}
			elif [ $_status -eq 255 ]
				then
					echo "[ERROR] Unknown remote host $SFTP_HOSTNAME" | tee -a ${LOG_FILE}
			else
					echo "[ERROR] Network issue. please try to re run the script once again" | tee -a ${LOG_FILE}
			fi
			
			echo "[ERROR] Getting Files from remote location $SFTP_USER@$SFTP_HOSTNAME:$_FTP_File_Path to incoming directory ${_Incoming_File_Path} is Unsuccessfull" | tee -a ${LOG_FILE}
			exit 1
				
	fi
}

#CALLING CHKFTPDELSTATUS
#chkftpdeltrg
#CALLING SETFEEDDIR
setfeeddir
#CALLING ARCHIVE FUNCTION TO CLEAN INCOMING AND OUTGOING DIRECTORIES
archivedirs
#CALLING SFTPGET FUNCTION TO GET FILES
sftpget

echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}
echo "[INFO] Ending ek_opsbi_arinc_ftp_import script successfully" | tee -a ${LOG_FILE}
echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}

exit 0;