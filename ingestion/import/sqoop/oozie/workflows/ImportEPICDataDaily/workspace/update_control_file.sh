# Date: 10/10/2017
# Author: Manu Mukundan
# Project: HELIX
# Team: OppEfficiency
# Location: BAS Dubai 20th Floor
# This bash script is developed as a part of an incremental data load bringing the daily data from EPIC database to HELIX raw layer
# It updates the control file with processed dates used by the currently running workflow after the sqoop job (step 2 in the workflow) is finished
# Installation: upload this script (generate-date_params.sh) to the workspace /apps/helix/conf/oozie/workspaces/wf_importepicdata/incremental

CONTROL_FILE=$1
START_DATE=$2
END_DATE=$3

echo CONTROL_FILE=${CONTROL_FILE}
echo START_DATE=${START_DATE}
echo END_DATE=${END_DATE}

echo -e "${START_DATE};${END_DATE}" | hdfs dfs -appendToFile - ${CONTROL_FILE}