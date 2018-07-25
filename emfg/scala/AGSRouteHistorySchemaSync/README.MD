## README

This is a spark application to perform schema sync for AGS Route.

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

AgsRouteHistorySchemaSync
Usage: spark-submit <spark-options> --class com.emirates.helix.AGSRouteHistorySchemaSyncDriver  agsroutehistoryschemasync-<jar version>.jar [options]

  -i, --ags_src_route_history_location <value>
                           Required parameter : AGS Route History Location from Decomposed
  -o, --ags_tgt_route_history_sync_location <value>
                           Required parameter : AGS Route History Location for Schema Sync - Modeled
  -p, --coalesce_value <value>
                           Optional parameter : Coalesce value
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  --help                   Print this usage message
