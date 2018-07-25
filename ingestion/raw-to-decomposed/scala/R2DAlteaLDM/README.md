## README

This is a spark application to move the altea ldm data from helix raw layer to decomposed layer. This supports common files like avro, parquet or snappy format
Reads the file, does the processing on the data, adds helix uuid and timestamp and moves to decomposed layer

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie action

##### Applicatoin usage and input parameter help:
`Usage: spark-submit <spark-options> --class com.emirates.helix.AlteaLdmR2DDriver altea-ldm-r2d-<jar version>.jar [options]`
```
  -i, --in_path <value>    Required parameter : Input data path to read data from HDFS
  -o, --out_path <value>   Required parameter : Output path to save data to HDFS
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  -p, --in_format avro OR parquet OR sequence
                           Required parameter : Input data format, avro, parquet or sequence file
  -r, --out_format avro OR parquet
                           Required parameter : Output data format, avro, parquet or sequence file
  -t, --part <value>       Required parameter : Number of partitions for coalesce
  --help                   Print this usage messagee
```