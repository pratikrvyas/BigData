## README

This is a spark application to perform satellite constructin of Actual Fuel Burn.

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

ActualFuelBurnSatelliteDaily
Usage: spark-submit <spark-options> --class com.emirates.helix.ActualFuelBurnDriver  actualfuelburnsatellitedaily-<jar version>.jar [options]

  -i, --actualfuelburn_src_ags_route_dedupe_location <value>
                           Required parameter : Actual Fuel Burn Source or AGS route Deduped Location
  -j, --flightmaster_current_location <value>
                           Required parameter : Flight Master current Location
  -x, --actualfuelburn_tgt_current_location <value>
                           Required parameter : Actual Fuel Burn Current Target Location
  -y, --actualfuelburn_tgt_ags_route_rejected_location <value>
                           Required parameter : Actual Fuel Burn Rejected Records Location
  -p, --coalesce_value <value>
                           Optional parameter : Coalesce value
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  --help                   Print this usage message
