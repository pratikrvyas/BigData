## README

Spark application to read raw data from from csv format and writes to decomposed layer by adding helix uuid and timestamp

##### Build the application and package it : 

`mvn clean package`
    This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help:
`Usage: spark-submit <spark-options> --class com.emirates.helix.RawCSVDriver generic-csvtoavro-r2d-<jar versoin>.jar [options]`
```
  -i, --in_path <value>    Required parameter : Input file path for raw data
  -o, --out_path <value>   Required parameter : Output file path for decomposed data
  -c, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  -d, --delimiter <value>  Optional parameter : Delimiter value to parse csv. By default it is comma(,)
  --help                   Print this usage message
```