#!/bin/bash -e

HDFS_PATH=$1
echo HELIX_DATE=`echo $HDFS_PATH | rev | cut -d '/' -f1 | rev`

