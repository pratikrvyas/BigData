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
--rawdata = LOAD  '${SOURCE_DIRECTORY}'  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

rawdata = LOAD   '${SOURCE_DIRECTORY}' USING org.apache.pig.piggybank.storage.CSVLoader();

rawdata_column = FOREACH rawdata  GENERATE (chararray)$0 AS AREA,(chararray)$1 AS NEW_AREA,(chararray)$2 AS CURRENT_REGION,(chararray)$3 AS NEW_REGION,(chararray)$4 AS ISO_CODE,(chararray)$5 AS  COUNTRY_NAME,(chararray)$6 AS NEW_COUNTRY_NAME,(chararray)$7 AS COUNTRY_CODE,(chararray)$8 AS NEW_COUNTRY_CODE,(chararray)$9 AS REPORTING_CURRENCY,(chararray)$10 AS NEW_REP_CURRENCY ;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdata_column GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, * ;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );