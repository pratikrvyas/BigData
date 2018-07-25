## README

This is a generic spark application developed to split specific columns from the input data and save the splitted columns to output locatoin. This is a specific
requiement for flight_gnome where the lido fsum data (incremental and historic) available in helix decomposed layer needs to be splitted to generate the flight
level info. But the application is created such that it can be used as a generic application where specific columns have to be filtered from the input data.
The output columns are defined in a configuration file whose location will be provided as an input parameter to the application

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help
```Usage: spark-submit --class com.emirates.helix.GenericColumnSplitDriver generic-columnsplit-.jar [options]
-i, --in_path Required parameter : Input file path
-o, --out_path Required parameter : Output file path
-x, --conf_file Required parameter : Config parameter file for the column names to select
-c, --compression snappy OR deflate
Required parameter : Output data compression format, snappy or deflate
-s, --in_fmt avro OR parquet
Required parameter : Input data format, avro or parquet
-p, --out_fmt avro OR parquet
Required parameter : Input data format, avro or parquet
--help Print this usage message```