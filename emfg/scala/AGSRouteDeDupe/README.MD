## README

This is a spark application to perform DeDupe logic for AGS Route.

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

AgsRouteDeDupe
Usage: spark-submit <spark-options> --class com.emirates.helix.AgsRouteDeDupeDriver  agsroutededupe-<jar version>.jar [options]

  -i, --ags_src_route_history_sync_location <value>
                           Required parameter : AGS route History Sync Location
  -j, --ags_src_route_incremental_location <value>
                           Required parameter : AGS route Incremental Data Location
  -x, --ags_tgt_route_dedupe_history_location <value>
                           Required parameter : AGS route DeDupe History Target Location
  -y, --ags_tgt_route_dedupe_current_location <value>
                           Required parameter : AGS route DeDupe Current Target Location
  -p, --coalesce_value <value>
                           Optional parameter : AGS route DeDupe coalesce value
  -m, --lookback_months <value>
                           Optional parameter : AGS route DeDupe look back value for segregating history and current
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  --help                   Print this usage message
