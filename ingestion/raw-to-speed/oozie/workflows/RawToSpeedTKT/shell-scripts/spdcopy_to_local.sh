#!/bin/bash -e

#---------------------------------------------------------------------------------------
# createIndex.sh
# Author : Jickson T George

# This shell script copies file from hdfs to flume spool directory
#---------------------------------------------------------------------------------------

hdfs dfs -copyToLocal $1/part-r-????? .
if [ $? -eq 0 ] 
then
	for f in part-r-?????; do 
		mv $f tkt_combined_$2_$3_$f
		#cp tkt_combined_$2_$3_$f /mnt/hnnfsshare/tktods/new
		cp tkt_combined_$2_$3_$f /mnt/hnnfsshare/spool_tktods/staging_dir
        #mv /mnt/hnnfsshare/tktods/new/tkt_combined_$2_$3_$f /mnt/hnnfsshare/tktods
        mv /mnt/hnnfsshare/spool_tktods/staging_dir/tkt_combined_$2_$3_$f /mnt/hnnfsshare/spool_tktods
	done
fi
