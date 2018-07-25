CONTROL_FILE=$1

echo CONTROL_FILE=${CONTROL_FILE}
echo START_DATE=`hdfs dfs -cat ${CONTROL_FILE}| tail -1| cut -d";" -f1`
echo END_DATE=`hdfs dfs -cat ${CONTROL_FILE}| tail -1| cut -d";" -f2`