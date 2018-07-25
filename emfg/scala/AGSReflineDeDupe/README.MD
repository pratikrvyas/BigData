## README

This is a spark application to perform DeDupe logic for AGS Refline.

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

AgsReflineDeDupe
Usage: spark-submit <spark-options> --class com.emirates.helix.AgsReflineDeDupeDriver  agsreflinededupe-<jar version>.jar [options]

  -i, --ags_src_refline_history_sync_location <value>
                           Required parameter : AGS Refline History Sync Location
  -j, --ags_src_refline_incremental_location <value>
                           Required parameter : AGS Refline Incremental Data Location
  -x, --ags_tgt_refline_dedupe_history_location <value>
                           Required parameter : AGS Refline DeDupe History Target Location
  -y, --ags_tgt_refline_dedupe_current_location <value>
                           Required parameter : AGS Refline DeDupe Current Target Location
  -p, --coalesce_value <value>
                           Optional parameter : AGS Refline DeDupe coalesce value
  -m, --lookback_months <value>
                           Optional parameter : AGS Refline DeDupe look back value for segregating history and current
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  --help                   Print this usage message
