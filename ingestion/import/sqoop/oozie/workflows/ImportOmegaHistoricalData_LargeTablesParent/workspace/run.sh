#! /bin/bash

PAR=$1
OVERWRITE=$2

tables_archive=(
OMG_STG_AGS
OMG_STG_LIDOMETAR_RT
OMG_STG_LIDO
CLC_DEAD_LOAD
CLC_FLIGHT_DTLS
CLC_LEG_INFO
CLC_LOAD_DETAIL
CLC_PAX_FIGURES
CLC_PAX_BAG_TOTAL
CLC_CREW_FIGURES
CLC_CABIN_SEC_INFO
MACS_DESTINATION
MACS_CARGO_DSTR_CMPT
MACS_LOAD_SHEET_DATA
)

tables=(
OMG_STG_LIDO
)

export OOZIE_URL=http://helixcloud-mn3.northeurope.cloudapp.azure.com:11000/oozie

running=()

for tab in "${tables[@]}"; 
do 
    #echo -e '\nStart processing: '$tab; 
    
    while IFS= read -r end
    do
        #echo $start;
		WAIT=1
        
        while [ ${WAIT} -eq 1 ]; do
		
			running_total=0;
			
			for i in ${running[@]}; do 
				if grep -q -P 'Status\s+:\s+RUNNING' <(oozie job -info $i); 
				then 
					(( ++running_total )); 
				else 
					delete=($i)
					running=("${running[@]/$delete}"); 
				fi
			done
			
			#echo 'running_total:'$running_total;
			
			if [ $running_total -ge $PAR ]; then sleep 1; else WAIT=0; fi
			
		done

		if [ -z ${END+x} ]; 
		then 
			START=1990-01-01;
		else 
			START=$END;
		fi
		END=$end; 
				
		#echo START=$START;
		#echo END=$END;

		###################################################################
		
		DIR=$( grep OMEGA_DIR ${tab}.properties | cut -d"=" -f2 | cut -d"$" -f1 )
		echo $DIR$START/_SUCCESS
		
		hdfs dfs -test -e $DIR$START/_SUCCESS
		
		if [[ $? -eq 0 && OVERWRITE -eq 0 ]]; then continue; fi
		
		
		###################################################################
		
		echo -e "\n\nProcessing ${tab}: \nSTART=${START}\nEND=${END}\n"
		
		NEW_START="START=${START}"
		NEW_END="END=${END}"
		
		sed -i "s@START=.*@$NEW_START@g" ${tab}.properties
		sed -i "s@END=.*@$NEW_END@g" ${tab}.properties
		
		#cat ${tab}.properties
		
		oozie_return=$(oozie job -config ${tab}.properties -run)
		oozie_job=$(echo $oozie_return | awk '{ gsub("job: ", "") } 1')
		running+=($oozie_job)
		echo 'Job started: http://10.232.154.13:8888/oozie/list_oozie_workflow/'$oozie_job

    done < "${tab}"  
    unset END
    
done


