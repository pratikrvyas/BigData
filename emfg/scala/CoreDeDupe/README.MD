## README

This is a spark application to perform DeDupe logic for Core.

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help:

CoreDeDupe
Usage: spark-submit <spark-options> --class com.emirates.helix.CoreDeDupeDriver  corededupe-<jar version>.jar [options]

  -i, --core_history_sync_location <value>
                           Required parameter : Core History Sync Location
  -j, --core_incremental_location <value>
                           Required parameter : Core Incremental Data Location
  -x, --core_dedupe_history_location <value>
                           Required parameter : Core DeDupe History Target Location
  -y, --core_dedupe_current_location <value>
                           Required parameter : Core DeDupe Current Target Location
  -p, --coalesce_value <value>
                           Optional parameter : Core DeDupe coalesce value
  -m, --lookback_months <value>
                           Optional parameter : Core DeDupe look back value for segregating history and current
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  --help                   Print this usage message
