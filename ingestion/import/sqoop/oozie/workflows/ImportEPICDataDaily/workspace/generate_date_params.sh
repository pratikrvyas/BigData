#!/bin/bash

# Date: 10/10/2017
# Author: Manu Mukundan
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
# This bash script is developed as a part of an incremental data load bringing daily EPIC data to HDFS
# It generates the start date from the control file
# Installation: upload this script (generate-date_params.sh) to the workspace /apps/helix/conf/oozie/workspaces/wf_importepicdata/incremental

CONTROL_FILE=$1
echo CONTROL_FILE=${CONTROL_FILE}
echo START_DATE=`hdfs dfs -cat ${CONTROL_FILE}| tail -1| cut -d";" -f2`
echo HELIX_TIMESTAMP=`date -u +"%s"`