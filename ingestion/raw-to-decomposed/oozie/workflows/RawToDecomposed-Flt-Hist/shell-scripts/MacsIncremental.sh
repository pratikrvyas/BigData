#!/bin/bash


INPUTDIR=$1
PROCESSINGDIR=$1/processing
PROCESSEDDIR=$1/processed
SCHEMADIR=$2  # /data/helix/schemas/macs/pax/PaxDetails-ver_1.0.avsc
#DECOMPOSEDDIR=$(echo $INPUTDIR | cut -f1,2,3 -d/)/decomposed/$(echo $INPUTDIR | cut -f5,6,7,8 -d/)
DECOMPOSEDDIR=$3
HADOOPNAMENODEPATH=$4
REJECTED_BASE_DIR="/data/helix/rejected/raw/macs"

if [[ $PROCESSINGDIR == *"/pax/"* ]]
then
  REJECTED_DIR=$REJECTED_BASE_DIR"/pax/"`basename $INPUTDIR`;
elif [[ $PROCESSINGDIR == *"/flt/"* ]]
then
  REJECTED_DIR=$REJECTED_BASE_DIR"/flt/"`basename $INPUTDIR`;
else
  REJECTED_DIR=$REJECTED_BASE_DIR"/dummy/"`basename $INPUTDIR`;
fi

echo "INPUTDIR=${INPUTDIR}"
echo "PROCESSINGDIR=${PROCESSINGDIR}"
echo "PROCESSEDDIR=${PROCESSEDDIR}"
echo "SCHEMADIR=${SCHEMADIR}"
echo "DECOMPOSEDDIR=${DECOMPOSEDDIR}"
echo "HOST=`hostname`"
echo "PWD=`pwd`"

#filesCount=`hadoop fs -ls $INPUTDIR/* | grep '^-' | wc -l`
hadoop fs -test -e ${INPUTDIR}/*.?????????????
testFilesExist=$?
if  [ "$testFilesExist" -ne 0 ]; then
        echo "no files to process... exiting !!!"
        exit 0
fi

hadoop fs -test -d $PROCESSINGDIR
testDir=$?
if  [ "$testDir" -ne 0 ]; then
    hadoop fs -mkdir $PROCESSINGDIR
	hadoop fs -chown :SCV_Group $PROCESSINGDIR
    hadoop fs -chmod ugo+rwx $PROCESSINGDIR
fi

hadoop fs -mv $INPUTDIR/*.????????????? $PROCESSINGDIR
echo "Command ::: hadoop fs -mv $INPUTDIR/*.????????????? $PROCESSINGDIR"
echo "file moved to processing !!! "

## Check for empty files in processing dir and move them to rejected
empty_files=`hadoop fs -ls $PROCESSINGDIR | awk '{ if ($5 == 0) print $8 }'`
if [ ${#empty_files[@]} -gt 0 ]; then
    hadoop fs -test -d $REJECTED_DIR
    testDir=$?
    if [ "$testDir" -ne 0 ]; then
       hadoop fs -mkdir -p $REJECTED_DIR
       hadoop fs -chown :SCV_Group $REJECTED_DIR
       hadoop fs -chmod ugo+rwx $REJECTED_DIR      
    fi
    for file in ${empty_files[@]}
    do
       hadoop fs -mv $file $REJECTED_DIR
    done
fi

str=`date +%s`
echo "str :::  $str"
echo "Command ::: hadoop jar SeqXmlToAvroHist.jar com.emirates.helix.macs.driver.SeqXmlToAvroUuid $PROCESSINGDIR $DECOMPOSEDDIR $SCHEMADIR $HADOOPNAMENODEPATH"
hadoop jar SeqXmlToAvroHist.jar com.emirates.helix.macs.driver.SeqXmlToAvroUuid $PROCESSINGDIR $DECOMPOSEDDIR $SCHEMADIR $HADOOPNAMENODEPATH
#hadoop jar SeqToAvroNew.jar com.emirates.helix.macs.driver.SeqXmlToAvroNew /data/helix/raw/MACS/testmove/ /data/helix/raw/MACS/avroout/out$str /data/helix/schemas/macs/pax/PaxDetails.avsc
status=$?
if [ "$status" -ne 0 ]; then
        hadoop fs -mv $PROCESSINGDIR/* $INPUTDIR/
		echo "hadoop job convert to avro failed !!! and files moved back to input dir !!!"
        exit -1
fi

echo "hadoop job successfully creates avro file !!!"
hadoop fs -test -d $PROCESSEDDIR
testDir=$?
if  [ "$testDir" -ne 0 ]; then
	hadoop fs -mkdir $PROCESSEDDIR
fi

hadoop fs -mv ${PROCESSINGDIR}/* $PROCESSEDDIR 
if [ $? -ne 0 ]; then
   echo "Moving files from processing to processed dir failed..!"
   exit -1
else 
   hadoop fs -rmr ${PROCESSINGDIR}
fi


echo R2D_OUTPUT_AVRO_FILE="$DECOMPOSEDDIR/$str/part-r-00000.avro"


