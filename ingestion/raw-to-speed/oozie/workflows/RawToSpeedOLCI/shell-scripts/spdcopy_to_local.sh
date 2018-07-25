#!/bin/bash -e

if [ -d olci_email ]; then
   rm -r olci_email
fi
if [ -d olci_sms ]; then
   rm -r olci_sms
fi

mkdir olci_email
mkdir olci_sms

cd olci_email
hdfs dfs -copyToLocal $1/part-m-????? .
if [ $? -eq 0 ] 
then
	for f in part-m-?????; do 
		mv $f olci_email_log_$3_$4_$f
		if [[ -s olci_email_log_$3_$4_$f ]] ; then
		   cp olci_email_log_$3_$4_$f /mnt/hnnfsshare/olciods/email/new
           mv /mnt/hnnfsshare/olciods/email/new/olci_email_log_$3_$4_$f /mnt/hnnfsshare/olciods/email
		fi;
	done
fi

cd ../olci_sms
hdfs dfs -copyToLocal $2/part-m-????? .
if [ $? -eq 0 ] 
then
	for f in part-m-?????; do 
		mv $f olci_sms_log_$3_$4_$f
		if [[ -s olci_sms_log_$3_$4_$f ]] ; then
		    cp olci_sms_log_$3_$4_$f /mnt/hnnfsshare/olciods/sms/new
            mv /mnt/hnnfsshare/olciods/sms/new/olci_sms_log_$3_$4_$f /mnt/hnnfsshare/olciods/sms
		fi ;	
	done
fi
