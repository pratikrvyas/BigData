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
rawdata = LOAD '${SOURCE_DIRECTORY}' AS (record:chararray); 

rawdatadelimited = foreach rawdata generate (chararray)SUBSTRING(record,0,3) AS CITY_CODE,(chararray)SUBSTRING(record,3, 6) AS FILLER1 ,(chararray)SUBSTRING(record,6,28) AS CITY_NAME,(chararray)SUBSTRING(record,28,30) AS STATE,(chararray)SUBSTRING(record,30,32) AS FILLER2,(chararray)SUBSTRING(record,32,34) AS COUNTRY_CODE,(chararray)SUBSTRING(record,34,36) AS TIME_ZONE_CODE,(chararray)SUBSTRING(record,36,37) AS STV,(chararray)SUBSTRING(record,37,39) AS FILLER3,(chararray)SUBSTRING(record,39,42) AS AIRPORT_CODE,(chararray)SUBSTRING(record,42,45) AS FILLER4,(chararray)SUBSTRING(record,45,67) AS AIRPORT_NAME,(chararray)SUBSTRING(record,67,69) AS FILLER5,(chararray)SUBSTRING(record,69,73) AS NUMERIC_CODE,(chararray)SUBSTRING(record,73,75) AS FILLER6,(chararray)SUBSTRING(record,75,76) AS LOCATION_TYPE;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdatadelimited GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, *;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );