#!/bin/bash -e

CONTROL_FILE=$1
RUNDATE=$2
WORKSPACE_PATH='/user/hue/oozie/workspaces/ws_ImportAncillaryCartrawlerData'
##CONTROL_FILE=DateCheckTestDataNew.txt
##RUNDATE=2018-03-30
echo START_DATE=`tail -n 1 ${CONTROL_FILE}`
START_DATE=`tail -n 1 ${CONTROL_FILE}`
echo "RUN_DATEE=${RUNDATE}"
echo "START_DATE=${START_DATE}"
if [[ "${START_DATE}" == "${RUNDATE}" ]]; 
then 
echo "HELIX_RUN_IMPORT=TRUE" 
else 
echo "HELIX_RUN_IMPORT=FALSE" 
fi
