-- pig script from raw to decomposed with uuid and timestamp 
-- register required libraries 
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

-- REGISTER /apps/DataLake/UDFJars/pigudf.jar;
REGISTER pigudf.jar;

-- load data 
avrodata = LOAD '${SOURCE_DIRECTORY}' 
		   USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', '${SKIP_INPUT_HEADER}') AS (SKYWARDSID: chararray, CITYOFRESIDENCE: chararray, COUNTRYOFRESIDENCE: chararray, EMAIL: chararray, SEPID: chararray, SEPUSERSDEVICES: chararray);

-- add uuid and timestamp
avrodatawithid = FOREACH avrodata GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, *;

SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;

-- store data in decomposed layer 
STORE avrodatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );
