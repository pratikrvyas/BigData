#!/bin/bash -e

CONTROL_FILE=$1
SQOOP_PATH=$2

echo CONTROL_FILE=${CONTROL_FILE}

echo END_DATE=`date +"%Y-%m-%d %H:%M:%S"`
echo START_DATE=`tail -n 1 ${CONTROL_FILE} | cut -d";" -f2`