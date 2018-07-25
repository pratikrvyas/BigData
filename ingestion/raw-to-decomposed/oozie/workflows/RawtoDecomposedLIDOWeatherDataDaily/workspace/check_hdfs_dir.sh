status=`hadoop fs -ls ${0}`
if [ ${status} = "" ] 
then
echo "[ERROR] HDFS output directory already exists"
exit 1
else
echo "[INFO] HDFS output directory created successfully"
fi
