## README

This is a spark application to perform DeDupe logic for Core.

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help:

CoreDeDupeDaily
Usage: spark-submit <spark-options> --class com.emirates.helix.CoreDeDupeDriver  corededupedaily-<jar version>.jar [options]

  -i, --core_dedupe_current_location <value>
                           Required parameter : Core DeDupe Current Location
  -j, --core_incremental_location <value>
                           Required parameter : Core Incremental Data Location
  -x, --core_dedupe_tgt_current_location <value>
                           Required parameter : Core DeDupe Target Current Location
  -p, --coalesce_value <value>
                           Optional parameter : Core DeDupe coalesce value
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  --help                   Print this usage message
