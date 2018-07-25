#!/bin/bash -e

CONTROL_FILE=/apps/helix/controlfiles/cris/cris_control.txt
START_DATE=$2
END_DATE=$3
echo CONTROL_FILE=${CONTROL_FILE}
echo START_DATE=${START_DATE}
echo END_DATE=${END_DATE}
echo "${START_DATE};${END_DATE}" | hdfs dfs -appendToFile - ${CONTROL_FILE}