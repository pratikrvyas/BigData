-- pig script from raw to decomposed with uuid and timestamp 
-- register required libraries 
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

REGISTER pigudf.jar;

--load data
rawdata = LOAD '${SOURCE_DIRECTORY2}' USING PigStorage('\n') AS (record:chararray); 

rawdatadelimited = foreach rawdata generate (chararray)SUBSTRING(record,0,21) AS NUMBER_OF_FILES,(chararray)SUBSTRING(record,21,104) AS NUMBER_OF_RECORD,(chararray)SUBSTRING(record,104,193) AS CONTROL_FILE_PROCESSING_DATE,(chararray)SUBSTRING(record,193,195) AS BACTH_NUMBER,(chararray)SUBSTRING(record,195,201) AS PROCESSING_DATE;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdatadelimited GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, *;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY2}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA2}' );