## README

This is a generic spark application to do the schema sync between the historic and incremental data. Schema sync is the process of converting the schema of historic data to match with incremental data. This is needed as a prerequisite to merge both historic and incremental data. The mapping between historic and incremental fields needs to be specified in a configuration file and the path for this file will be in input parameter for the application. This will be comma seperated file with first column as "HISTORIC" and second column as "INCREMENTAL". If historic data does not have any columns to match with incremental, the incremental columns will be added to historic data with null values. Here is the example for the file

```
HISTORY,INCREMENTAL <------------ HEADER
DEP_RUNWAY,DepRun
ARR_RUNWAY,ArrRun <------------ Field name in historic data with "ARR_RUNWAY" will be renamed as "ArrRun" to match incremental schema
CID,CosIndEcoCru
ALTN_IATA_1,FirstAltIat
ALTN_IATA_2,SecondAltIat
ALTN_IATA_3,ThirdAltIat
,tibco_message_time
,AirRadCalSig <------------ Historic data does not have column itself. In this case, new column "AirRadCalSig" will be added to historic data to match incremental schema
,AtcCal
,DatOfMes
,EtoAddFu
```

##### Limitations : 
Currently this application only supports syncing historic and incremental data, with one-to-one mapping. If the incremental data has a complex type and
historic data fields have to be joined to create the complex type, currently this application cannot be used.

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help
`Usage: spark-submit --class com.emirates.helix.SchemaSyncDriver schema-sync-.jar [options]`
```
-i, --in_path Required parameter : Input file path
-o, --out_path Required parameter : Output file path
-x, --mapping_file 
Required parameter : Mapping file with historic and incremental column mapping
-c, --compression snappy OR deflate
Required parameter : Output data compression format, snappy or deflate
-s, --in_fmt avro OR parquet
Required parameter : Input data format, avro or parquet
-p, --out_fmt avro OR parquet
Required parameter : Output data format, avro or parquet
--help Print this usage message
```