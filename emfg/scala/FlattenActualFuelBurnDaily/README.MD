## README

This is a spark application to perform Flattenning ActualFuelBurn Satellite.

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action


FlattenActualFuelBurn
Usage: spark-submit <spark-options> --class com.emirates.helix.FlattenActualFuelBurnDriver  flattenactualfuelburn-<jar version>.jar [options]

  -i, --actualfuelburn_src_location <value>
                           Required parameter : Actual Fuel Burn Satellite Source Location
  -j, --control_file <value>
                           Required parameter : Control File Path
  -x, --actualfuelburn_tgt_serving_location <value>
                           Required parameter : Actual Fuel Burn Target Location of Serving Layer
  -p, --coalesce_value <value>
                           Optional parameter : Coalesce value
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  --help                   Print this usage message
