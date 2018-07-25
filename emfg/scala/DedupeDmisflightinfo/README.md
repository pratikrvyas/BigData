## README

Spark application for performing deduplication of DMIS flight info data. This is a single application which can be used for doing the dedup for the first day 
(history + incremental) dedup and the subsequent dedupes from day two onwards (current deduped data + latest incremental)

##### Build the application and package it : 

`mvn clean package`
    This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help:
`Usage: spark-submit <spark-options> --class com.emirates.helix.DmisFlightLevelDedupDriver dmis-flightlevel-dedup-<jar version>.jar [options]`
```
  -i, --incr_path <value>  Required parameter : Incremental data input path
  -h, --hist_path <value>  Required parameter : Input path for historic data (History data on day 1 and deduped data from day 2 onwards
  -o, --h_out_path <value>
                           Optional parameter : Output file path for history data store needed for initial run only
  -l, --l_out_path <value>
                           Required parameter : Output file path for latest/current data store
  -m, --months <value>     Optional parameter : History and current data split month value for initial run. Default : 12 months
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  -p, --part <value>       Required parameter : Number of partitions for coalesce
  -t, --init               Use this option if it is the first time to load both history and incremental
  --help                   Print this usage message
  ```