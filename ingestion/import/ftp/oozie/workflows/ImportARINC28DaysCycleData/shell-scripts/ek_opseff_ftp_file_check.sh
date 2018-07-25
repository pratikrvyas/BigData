#-------------------------------------------------------------------------------
# Script name : ek_opseff_ftp_file_check.sh                        
# Author      : Ravindra Chellubani                
# Created on  : 27-03-2018
# Modified on : 
# Modified by : Ravindra Chellubani
# Description : This script is used to check wheter file exists or not in ftp
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
#Run cmds
#case 1: Runnning with Default params
#sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_ftp_file_check.sh -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env
#--------------------------------------------------------------------------------

#MAIN EXECUTION BEGINS
echo "------------------------------------------------------------------"
echo "[INFO] Starting ek_opsbi_arinc_ftp_import script"
echo "------------------------------------------------------------------"

#FUNCTION TO CHECKING IF PARAMETERS ARE SPECIFIED CORRECTLY OR NOT
function cmd_line_argument_error()
{
	echo "[ERROR] Please make sure that no. of arguments are passed correctly"
	echo "[INFO] If running with default params : sh /home/ops_eff/ek_arinc/arinc_scripts/ek_opseff_ftp_file_check.sh -p /home/ops_eff/ek_arinc/arinc_params/ek_opseff_arinc_import.env"
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

_FTP_File_Path=${SFTP_ARINC_FILE_PATH}

#TO CHECK IF FTP FILE EXISTS OR NOT
sftp $SFTP_USER@$SFTP_HOSTNAME:$_FTP_File_Path/ <<< "ls" | tail -1 | grep -v '^sftp>' > trg.txt

file_flag=`cat trg.txt | wc -l`

if [ ${file_flag} -eq '0' ]
  then
    echo "[INFO] No files found in ftp path ${_FTP_File_Path}" | tee -a ${LOG_FILE}
    exit 0;
else
    echo "[INFO] files found in ftp path ${_FTP_File_Path}" | tee -a ${LOG_FILE}
fi
    

