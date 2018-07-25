#!/bin/bash -e

CURRENT_DATE="$1"
CURRENT_TIME="$2"
INPUT_FILE="$3"
RAW_PATH="$4"
DECOMP_PATH="$5"

echo "CURRENT_DATE=${CURRENT_DATE}"
echo "CURRENT_TIME=${CURRENT_TIME}"
echo "INPUT_FILE=${INPUT_FILE}"
echo "RAW_PATH=${RAW_PATH}"
echo "DECOMP_PATH=${DECOMP_PATH}"

while read -r DIRECTORY_NAME; do

	SRC_PATH="${RAW_PATH}/${DIRECTORY_NAME}/incremental/${CURRENT_DATE}/${CURRENT_TIME}"
	DEST_PATH="${DECOMP_PATH}/${DIRECTORY_NAME}/incremental/${CURRENT_DATE}/${CURRENT_TIME}"
	
	hadoop fs -mkdir -p $DEST_PATH
	echo "Created decomposed directory ${DEST_PATH}"

	hadoop fs -cp ${SRC_PATH}/* $DEST_PATH/
	echo "Copied from ${SRC_PATH} to ${DEST_PATH}"

done < "$INPUT_FILE"
