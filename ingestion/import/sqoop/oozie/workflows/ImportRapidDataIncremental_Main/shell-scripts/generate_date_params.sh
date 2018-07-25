CONTROL_FILE=$1

echo CONTROL_FILE=${CONTROL_FILE}
echo END_DATE=`date -d '1 day ago' +"%Y-%m-%d 23:59:59"`
echo END_TIME=`date +"%Y-%m-%d 00:00:00"`
echo START_DATE=`hdfs dfs -cat ${CONTROL_FILE} | tail -n 1 | cut -d";" -f2`

if [ `date +"%-H"` -lt 2 ]
then
	echo "HELIX_RUN_IMPORT=FALSE"
else
	echo "HELIX_RUN_IMPORT=TRUE" 
fi
