## README

This is a spark application to move the data in helix raw layer to decomposed layer. This supports common files like avro, parquet and csv
Reads the file, adds helix uuid and timestamp and moves to decomposed layer

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help:
`Usage: spark-submit <spark-options> --class com.emirates.helix.MultiSourceR2DDriver multisource-r2d-<jar version>.jar [options]`
```
  -i, --in_path <value>    Required parameter : Input file path for raw data
  -o, --out_path <value>   Required parameter : Output file path for decomposed data
  -d, --drop <value>       Optional parameter : If a column from input source has to be removed. Specify the column name
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  -s, --in_fmt avro, parquet OR csv
                           Required parameter : Input data format, avro, parquet, csv or sequence file
  -p, --out_fmt avro OR parquet
                           Required parameter : Input data format, avro or parquet
  -r, --part <value>       Required parameter : Number of partitions for coalesce
  --help                   Print this usage message
```