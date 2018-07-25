## README

This is a spark application to perform SchemaSync for AGS Refline.

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help:

AgsReflineHistorySchemaSync
Usage: spark-submit <spark-options> --class com.emirates.helix.AgsReflineHistorySchemaSyncDriver  agsreflinehistoryschemasync-<jar version>.jar [options]

  -i, --ags_src_refline_history_location <value>
                           Required parameter : AGS Refline History Location from Decomposed
  -o, --ags_tgt_refline_history_sync_location <value>
                           Required parameter : AGS Refline History Location for Schema Sync - Modeled
  -p, --coalesce_value <value>
                           Optional parameter : Core DeDupe coalesce value
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  --help                   Print this usage message
