## README

This is a spark application to move the dmis incremental decomposed data to the modelled layer, prerequisite folder. This process is needed to clean the decomposed data
so that it can be successfully used in the following processes like dedup

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help:
`Usage: spark-submit <spark-options> --class com.emirates.helix.DmisFormatIncrSchemaDriver dmis-sync-prereq-<jar version>.jar [options]`

```
  -i, --in_path <value>    Required parameter : Input data path
  -o, --out_path <value>   Required parameter : Output file path
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  -s, --in_fmt avro OR parquet
                           Required parameter : Input data format, avro or parquet
  -p, --out_fmt avro OR parquet
                           Required parameter : Output data format, avro or parquet
  --help                   Print this usage message
```