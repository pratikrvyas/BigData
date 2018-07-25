#!/bin/bash -e

echo "AVRO_SCHEMA=`hdfs dfs -cat $1 | tr -d '\n\r'`"
