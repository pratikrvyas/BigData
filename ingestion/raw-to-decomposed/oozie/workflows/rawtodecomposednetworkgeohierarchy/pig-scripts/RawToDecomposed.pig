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

rawdata_column = FOREACH rawdata  GENERATE (chararray)$0 AS EFFECTIVE_DATE,(chararray)$1 AS SNO,(chararray)$2 AS CITY_CODE,(chararray)$3 AS CITY,(chararray)$4 AS COUNTRY_CODE,(chararray)$5 AS  COUNTRY_NAME,(chararray)$6 AS REGION_CODE,(chararray)$7 AS REGION,(chararray)$8 AS REPORTING_CURRENCY,(chararray)$9 AS ISO_CODE,(chararray)$10 AS MAIN_PORT,(chararray)$11 AS AREA_CODE,(chararray)$12 AS  AREA_NAME;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdata_column GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, * ;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );