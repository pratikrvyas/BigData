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
		mv $f pnr_combined_$2_$3_$f
		#cp pnr_combined_$2_$3_$f /mnt/hnnfsshare/pnrods/new/
		cp pnr_combined_$2_$3_$f /mnt/hnnfsshare/spool_pnrods/staging_dir/
        #mv /mnt/hnnfsshare/pnrods/new/pnr_combined_$2_$3_$f /mnt/hnnfsshare/pnrods
        mv /mnt/hnnfsshare/spool_pnrods/staging_dir/pnr_combined_$2_$3_$f /mnt/hnnfsshare//spool_pnrods
	done
fi
