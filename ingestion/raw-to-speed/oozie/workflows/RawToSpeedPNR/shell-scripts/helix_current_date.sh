#!/bin/bash -e

SQOOP_PATH=$1
echo HELIX_DATE=`echo $SQOOP_PATH | rev | cut -d '/' -f2 | rev`
echo HELIX_TIME=`echo $SQOOP_PATH | rev | cut -d '/' -f1 | rev`
