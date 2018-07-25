#!/bin/bash

# -----------------------------------------------
#
#       Developer:         Satish Reddy ( S751237)
#       Purpose:         identifies the incremental files and copies them to HDFS
#
# ---------------------------------------------------


SD_PNR=$1
SD_PNR_CONTROL=$2
SD_PNR_RAW=$3
SD_PNRCTRL_RAW=$4
SD_AGENT=$5
SD_AGENT_CONTROL=$6
SD_AGENT_RAW=$7
SD_AGENTCTRL_RAW=$8
REJECTEDDEC_PNR=$9
REJECTEDDEC_PNRCTRL=${10}
REJECTEDRAW_PNR=${11}
REJECTEDRAW_PNRCTRL=${12}
REJECTEDDEC_AGENT=${13}
REJECTEDDEC_AGENTCTRL=${14}
REJECTEDRAW_AGENT=${15}
REJECTEDRAW_AGENTCTRL=${16}

execution_date=`date +"%Y%m%d%H%M%S"`

export HADOOP_USER_NAME=scv_ops

hdfs dfs -mkdir -p ${REJECTEDDEC_PNR}/$execution_date
hdfs dfs -mkdir -p ${REJECTEDDEC_PNRCTRL}/$execution_date
hdfs dfs -mkdir -p ${REJECTEDRAW_PNR}/$execution_date
hdfs dfs -mkdir -p ${REJECTEDRAW_PNRCTRL}/$execution_date
hdfs dfs -mkdir -p ${REJECTEDDEC_AGENT}/$execution_date
hdfs dfs -mkdir -p ${REJECTEDDEC_AGENTCTRL}/$execution_date
hdfs dfs -mkdir -p ${REJECTEDRAW_AGENT}/$execution_date
hdfs dfs -mkdir -p ${REJECTEDRAW_AGENTCTRL}/$execution_date



hdfs dfs -cp -f ${SD_PNR}/* ${REJECTEDDEC_PNR}/$execution_date
hdfs dfs -cp -f ${SD_PNR_CONTROL}/* ${REJECTEDDEC_PNRCTRL}/$execution_date
hdfs dfs -cp -f ${SD_PNR_RAW}/* ${REJECTEDRAW_PNR}/$execution_date
hdfs dfs -cp -f ${SD_PNRCTRL_RAW}/* ${REJECTEDRAW_PNRCTRL}/$execution_date
hdfs dfs -cp -f ${SD_AGENT}/* ${REJECTEDDEC_AGENT}/$execution_date
hdfs dfs -cp -f ${SD_AGENT_CONTROL}/* ${REJECTEDDEC_AGENTCTRL}/$execution_date
hdfs dfs -cp -f ${SD_AGENT_RAW}/* ${REJECTEDRAW_AGENT}/$execution_date
hdfs dfs -cp -f ${SD_AGENTCTRL_RAW}/* ${REJECTEDRAW_AGENTCTRL}/$execution_date

SD_PNR_cut=`echo ${SD_PNR} | rev | cut -d '/' -f1- | rev` 
hdfs dfs -rm -r $SD_PNR_cut

SD_PNR_CONTROL_cut=`echo ${SD_PNR_CONTROL} | rev | cut -d '/' -f1- | rev` 
hdfs dfs -rm -r $SD_PNR_CONTROL_cut

SD_PNR_RAW_cut=`echo ${SD_PNR_RAW} | rev | cut -d '/' -f1- | rev` 
hdfs dfs -rm -r $SD_PNR_RAW_cut

SD_PNRCTRL_RAW_cut=`echo ${SD_PNRCTRL_RAW} | rev | cut -d '/' -f1- | rev` 
hdfs dfs -rm -r $SD_PNRCTRL_RAW_cut

SD_AGENT_cut=`echo ${SD_AGENT} | rev | cut -d '/' -f1- | rev` 
hdfs dfs -rm -r $SD_AGENT_cut

SD_AGENT_CONTROL_cut=`echo ${SD_AGENT_CONTROL} | rev | cut -d '/' -f1- | rev` 
hdfs dfs -rm -r $SD_AGENT_CONTROL_cut

SD_AGENT_RAW_cut=`echo ${SD_AGENT_RAW} | rev | cut -d '/' -f1- | rev` 
hdfs dfs -rm -r $SD_AGENT_RAW_cut

SD_AGENTCTRL_RAW_cut=`echo ${SD_AGENTCTRL_RAW} | rev | cut -d '/' -f1- | rev` 
hdfs dfs -rm -r $SD_AGENTCTRL_RAW_cut