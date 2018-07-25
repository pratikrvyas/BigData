#!/bin/bash -e

CURRENT_DATE="$1"
CURRENT_TIME="$2"
INPUT_FILE="$3"
RAW_PATH="$4"
DECOMP_PATH="$5"
HDFS_URL="$6"
HIVE_DATABASE="$7"

echo "CURRENT_DATE=${CURRENT_DATE}"
echo "CURRENT_TIME=${CURRENT_TIME}"
echo "INPUT_FILE=${INPUT_FILE}"
echo "RAW_PATH=${RAW_PATH}"
echo "DECOMP_PATH=${DECOMP_PATH}"

while read -r DIRECTORY_NAME SQOOP_STYLE; do

	if [[ "$SQOOP_STYLE" == "incremental" ]]; then
		SRC_PATH="${RAW_PATH}/${DIRECTORY_NAME}/incremental/${CURRENT_DATE}/${CURRENT_TIME}"
		DEST_PATH="${DECOMP_PATH}/${DIRECTORY_NAME}/incremental/${CURRENT_DATE}/${CURRENT_TIME}"
		hadoop fs -mkdir -p $DEST_PATH
		hadoop fs -cp ${SRC_PATH}/* $DEST_PATH/
	elif [[ "$SQOOP_STYLE" == "full" ]]; then
		SRC_PATH="${RAW_PATH}/${DIRECTORY_NAME}/full/${CURRENT_DATE}"
		DEST_PATH="${DECOMP_PATH}/${DIRECTORY_NAME}/full/${CURRENT_DATE}"
		hadoop fs -mkdir -p $DEST_PATH
		hadoop fs -cp ${SRC_PATH}/* $DEST_PATH/
		TABLE_NAME="${DIRECTORY_NAME}"
		TABLE_PATH="${HDFS_URL}${DEST_PATH}"
		hive --database=$HIVE_DATABASE -e "alter table $TABLE_NAME set location '${TABLE_PATH}'"
	fi		

done < "$INPUT_FILE"
