#!/bin/bash -e

echo "AVRO_SCHEMA=`hdfs dfs -cat $1 2>/dev/null | tr -d '\n\r' | sed 's/^\([^{]*\)\([{].*\)$/\2/'`"
