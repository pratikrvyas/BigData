-- pig script from raw to decomposed with uuid and timestamp 
-- register required libraries 
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

REGISTER  pigudf.jar;

--load data
rawdata = LOAD  '${SOURCE_DIRECTORY}'  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

rawdata_column = FOREACH rawdata  GENERATE (chararray)$0 AS Countrycode,(chararray)$1 AS Airline_numeric_number,(chararray)$2 AS OAL_Cabin,(chararray)$3 AS OAL_Cabin_name,(chararray)$4 AS EK_Cabin,(chararray)$5 AS EK_Cabin_name,(chararray)$6 AS Effective_Start_Date,(chararray)$7 AS  Effective_End_Date,(chararray)$8 AS  Default_Flag;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdata_column GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, * ;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );