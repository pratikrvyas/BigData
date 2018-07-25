#!/bin/bash

SOURCE_DIR=$1
DEST_DIR=$2

# The data is a combination of OLCI and Reminder SMS. Filter only OLCI SMS records and remove non-ascii characters
hadoop fs -cat $SOURCE_DIR/*  | tr -cd '\001-\177' | grep -v 'EMIRATES FLIGHT REMINDER' | hadoop fs -put -  $DEST_DIR/olci_sms.csv