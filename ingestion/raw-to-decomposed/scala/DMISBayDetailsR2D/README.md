## README

This is a spark application to move the dmis bay details data from helix raw layer to decomposed layer. This supports common files like avro, parquet or snappy format
Reads the file, adds helix uuid and timestamp and moves to decomposed layer

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help:
`Usage: spark-submit <spark-options> --class com.emirates.helix.DmisBayHistoryR2DDriver dmis-bayhistory-r2d-<jar version>.jar [options]`
```
  -i, --in_path <value>    Required parameter : Input file path
  -o, --out_path <value>   Required parameter : Output file path
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  -p, --part <value>       Required parameter : Number of partitions for coalesce
  -r, --in_format avro/parquet/sequence
                           Required parameter : Input data format, avro or parquet or sequence
  -s, --out_format avro OR parquet
                           Required parameter : Output data format, avro or parquet
  --help                   Print this usage message
```