#-------------------------------------------------------------------------------
# Script name : ek_opseff_ags_ftp_import_daily.sh                        
# Author      : Ravindra Chellubani                
# Created on  : 13-09-2017
# Modified on : 13-09-2017
# Modified by : Ravindra Chellubani
# Description : This script downloads AGS related files to Helix datalake on daily 
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
#Run cmds
#case 1: Runnning with Default params
#sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_ftp_import_daily.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f <idle_reverse|single_engine|nominal_speed|actual_route_with_refline|cruise_climb_and_cruise_descent_analysis>
#case 2: To Manually process single file
#sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_ftp_import_daily.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f <idle_reverse|single_engine|nominal_speed|actual_route_with_refline|cruise_climb_and_cruise_descent_analysis> -f <idle_reverse|single_engine|nominal_speed|actual_route_with_refline|cruise_climb_and_cruise_descent_analysis> -n <filename>
#--------------------------------------------------------------------------------

#CASE 1 SAMPLE RUN CMD
#sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_ftp_import_daily.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f idle_reverse

#MAIN EXECUTION BEGINS
echo "------------------------------------------------------------------"
echo "[INFO] Starting ek_opsbi_ags_ftp_import script"
echo "------------------------------------------------------------------"

#FUNCTION TO CHECKING IF PARAMETERS ARE SPECIFIED CORRECTLY OR NOT
function cmd_line_argument_error()
{
	echo "[ERROR] Please make sure that no. of arguments are passed correctly"
	echo "[INFO] If running with default params : sh /home/ops_eff/ek_ags/ags_scripts/ek_opseff_ags_ftp_import_daily.sh -p /home/ops_eff/ek_ags/ags_params/ek_opseff_ags_import.env -f <idle_reverse|single_engine|nominal_speed|actual_route_with_refline|cruise_climb_and_cruise_descent_analysis>"
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
echo "[INFO] Creating AGS log file successfully"
_var_Date=`date +%d%m%y%OH%OM%OS`
_var_Date_Only=`date +%d%m%Y`
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

#FUNCTION TO CHECK TRIGGER
function chkftpdeltrg(){

	echo "[INFO] Checking the success trigger file for proceeding further. This check avoids duplicates in raw layer" | tee -a ${LOG_FILE}
	
	if [ ! -f "${AGS_STATUS_FILE_PATH}/$_Feed_Name/${AGS_STATUS_FILE_NAME}" ]
	then
		echo "[ERROR] Previous run was unsuccessfull success trigger doesn't exist in ${AGS_STATUS_FILE_PATH}/$_Feed_Name/. please check " | tee -a ${LOG_FILE}
		exit 1
	else	
		echo "[INFO] Previous run was successfull success trigger exist in ${AGS_STATUS_FILE_PATH}/$_Feed_Name/. Proceeding with next steps.. " | tee -a ${LOG_FILE}
	fi	
	
}


#FUNCTION TO SET FEED DIRECTORIES
function setfeeddir(){

echo "[INFO] Identifying $_Feed_Name feed directries" | tee -a ${LOG_FILE}
	if [ $_Feed_Name == "${AGS_FEED_RV}" ]
		then
			_FTP_File_Path=${AGS_SFTP_RV_FILE_PATH}
			_Incoming_File_Path=${AGS_INCOMING_FILE_PATH}/${AGS_FEED_RV}
			_Outgoing_File_Path=${AGS_OUTGOING_FILE_PATH}/${AGS_FEED_RV}
			_Archive_File_Path=${AGS_ARCHIVE_FILE_PATH}/${AGS_FEED_RV}
			_Feed_Name_Short=${AGS_FEED_RV_SHORT}
	elif [ $_Feed_Name == "${AGS_FEED_SE}" ]
		then
			_FTP_File_Path=${AGS_SFTP_SE_FILE_PATH}
			_Incoming_File_Path=${AGS_INCOMING_FILE_PATH}/${AGS_FEED_SE}
			_Outgoing_File_Path=${AGS_OUTGOING_FILE_PATH}/${AGS_FEED_SE}
			_Archive_File_Path=${AGS_ARCHIVE_FILE_PATH}/${AGS_FEED_SE}
			_Feed_Name_Short=${AGS_FEED_SE_SHORT}
	elif [ $_Feed_Name == "${AGS_FEED_N1}" ]
		then
			_FTP_File_Path=${AGS_SFTP_N1_FILE_PATH}
			_Incoming_File_Path=${AGS_INCOMING_FILE_PATH}/${AGS_FEED_N1}
			_Outgoing_File_Path=${AGS_OUTGOING_FILE_PATH}/${AGS_FEED_N1}
			_Archive_File_Path=${AGS_ARCHIVE_FILE_PATH}/${AGS_FEED_N1}
			_Feed_Name_Short=${AGS_FEED_N1_SHORT}
	elif [ $_Feed_Name == "${AGS_FEED_ACTUAL}" ]
		then
			_FTP_File_Path=${AGS_SFTP_ACTUAL_FILE_PATH}
			_Incoming_File_Path=${AGS_INCOMING_FILE_PATH}/${AGS_FEED_ACTUAL}
			_Outgoing_File_Path=${AGS_OUTGOING_FILE_PATH}/${AGS_FEED_ACTUAL}
			_Archive_File_Path=${AGS_ARCHIVE_FILE_PATH}/${AGS_FEED_ACTUAL}
			_Feed_Name_Short=${AGS_FEED_ACTUAL_SHORT}
	else
			echo "[ERROR] Unknown feed name. please check the feed name <idle_reverse|single_engine|nominal_speed|actual_route_with_refline|cruise_climb_and_cruise_descent_analysis>"
			exit 1;
	fi
	
	echo "[INFO] Successfully identified the feed directries. FTP PATH : $_FTP_File_Path, INCOMING PATH : $_Incoming_File_Path, OUTGOING PATH : $_Outgoing_File_Path, ARCHIVE PATH : $_Archive_File_Path" | tee -a ${LOG_FILE}

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
	
	echo "[INFO] Cleaning files in outgoing directory ${_Outgoing_File_Path}" | tee -a ${LOG_FILE}
	echo "[INFO] List of cleaned files: " $(ls -R ${_Outgoing_File_Path}) | tee -a ${LOG_FILE}
	rm -r ${_Outgoing_File_Path}/*
	_status=$?
	if [ $_status -eq 0 ]
		then
			echo "[INFO] Cleaning files in outgoing directory ${_Outgoing_File_Path} were successfully processed" | tee -a ${LOG_FILE}

	else	
			echo "[ERROR] Cleaning files in outgoing directory ${_Outgoing_File_Path} is Unsuccessfull" | tee -a ${LOG_FILE}
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

	echo "[INFO] Getting files from remote location $AGS_SFTP_USER@$AGS_SFTP_HOSTNAME:$_FTP_File_Path to incoming directory ${_Incoming_File_Path}"  | tee -a ${LOG_FILE}
	
	#FTP CMD TO GET FILES FROM THE REMOTE HOST
	sftp $AGS_SFTP_USER@$AGS_SFTP_HOSTNAME:$_FTP_File_Path/*.zip ${_Incoming_File_Path}
	_status=$?
	if [ $_status -eq 0 ]
		then
			#FTP VALIDATING
			_count=`ls  ${_Incoming_File_Path} | grep -v -i ${_Feed_Name_Short}_[0-9]* | wc -l`
			if [ $_count -gt 0 ]
			then
				echo "[ERROR] Getting files from remote location $AGS_SFTP_USER@$AGS_SFTP_HOSTNAME:$_FTP_File_Path to incoming directory ${_Incoming_File_Path} is unsuccessfull : FTP Directory($_FTP_File_Path) contains other feed files"  | tee -a ${LOG_FILE}
				exit 1
			fi
			echo "[INFO] List of files: " | tee -a ${LOG_FILE}
			echo `ls ${_Incoming_File_Path}` | tee -a ${LOG_FILE}
			echo "[INFO] Getting files from remote location $AGS_SFTP_USER@$AGS_SFTP_HOSTNAME:$_FTP_File_Path to incoming directory ${_Incoming_File_Path} were successfully processed" | tee -a ${LOG_FILE}

	else
			if [ $_status -eq 1 ]
				then
					echo "[ERROR] Unknown remote file path $AGS_SFTP_USER@$AGS_SFTP_HOSTNAME:$_FTP_File_Path" | tee -a ${LOG_FILE}
			elif [ $_status -eq 255 ]
				then
					echo "[ERROR] Unknown remote host $AGS_SFTP_HOSTNAME" | tee -a ${LOG_FILE}
			else
					echo "[ERROR] Network issue. please try to re run the script once again" | tee -a ${LOG_FILE}
			fi
			
			echo "[ERROR] Getting Files from remote location $AGS_SFTP_USER@$AGS_SFTP_HOSTNAME:$_FTP_File_Path to incoming directory ${_Incoming_File_Path} is Unsuccessfull" | tee -a ${LOG_FILE}
			exit 1
				
	fi
}

#FUNCTION UNZIPPING AND MOVE FILES TO OUTGOING DIRECTORY
function unzipfiles(){

	echo "[INFO] Cheching if the outgoing directory exists or not. otherwise creates a outgoing directory : ${_Outgoing_File_Path}" | tee -a ${LOG_FILE}
	if [ ! -d "${_Outgoing_File_Path}" ]
		then
			mkdir -p ${_Outgoing_File_Path}
			echo "[INFO] Created outgoing directory" | tee -a ${LOG_FILE}
	else
			echo "[INFO] Outgoing directory exists" | tee -a ${LOG_FILE}
	fi

	echo "[INFO] Unzipping files in incoming directory ${_Incoming_File_Path} and moving to outgoing directory ${_Outgoing_File_Path}"  | tee -a ${LOG_FILE}

	#UNZIP CMD 
	#unzip -o -d ${_Outgoing_File_Path}/ ${_Incoming_File_Path}/"*.zip"
	for file in "${AGS_INCOMING_FILE_PATH}/$_Feed_Name/"* ; 
	do
		filename=$(echo `basename $file` | cut -f 1 -d '.')
		unzip -d "${_Outgoing_File_Path}/$filename" "$file" 
		_status=$?

		if [ $_status -eq 0 ]
			then
				#echo "[INFO] List of unzip files: " $(ls -R ${_Outgoing_File_Path}) | tee -a ${LOG_FILE}
				echo "[INFO] Unzipping files in incoming directory ${_Incoming_File_Path} and moving to outgoing directory ${_Outgoing_File_Path}/${_var_Date_Only} were successfully processed" | tee -a ${LOG_FILE}

		else	
				echo "[ERROR] Unzipping files in incoming directory ${_Incoming_File_Path} and moving to outgoing directory ${_Outgoing_File_Path} is Unsuccessfull" | tee -a ${LOG_FILE}
				exit 1
					
		fi
	done
	#deleting status file
	rm -r ${AGS_STATUS_FILE_PATH}/$_Feed_Name/*
	echo "[INFO] successfully removed trigger file ${AGS_STATUS_FILE_PATH}/$_Feed_Name/${AGS_STATUS_FILE_NAME}." | tee -a ${LOG_FILE}
}

#CALLING CHKFTPDELSTATUS
#chkftpdeltrg
#CALLING SETFEEDDIR
setfeeddir
#CALLING ARCHIVE FUNCTION TO CLEAN INCOMING AND OUTGOING DIRECTORIES
archivedirs
#CALLING SFTPGET FUNCTION TO GET FILES
sftpget
#CALLING UNZIPFILES FUNCTION
unzipfiles

echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}
echo "[INFO] Ending ek_opsbi_ags_ftp_import script successfully" | tee -a ${LOG_FILE}
echo "------------------------------------------------------------------" | tee -a ${LOG_FILE}

exit 0;