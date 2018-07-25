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
rawdata = LOAD '${SOURCE_DIRECTORY1}' USING PigStorage('\n') AS (record:chararray);  

rawdatadelimited = foreach rawdata generate (chararray)SUBSTRING(record,0,1) AS Gds_code,(chararray)SUBSTRING(record,1, 10) AS Pseudo_City_Code,(chararray)SUBSTRING(record,10,17) AS Agency_Number,(chararray)SUBSTRING(record,17,18) AS Check_digit,(chararray)SUBSTRING(record,18,58) AS Name,(chararray)SUBSTRING(record,58,98) AS Address_1,(chararray)SUBSTRING(record,98,138) AS Address_2,(chararray)SUBSTRING(record,138,178) AS City,(chararray)SUBSTRING(record,178,181) AS State,(chararray)SUBSTRING(record,181,191) AS Postal_code,(chararray)SUBSTRING(record,191,193) AS Country,(chararray)SUBSTRING(record,193,200) AS FILLER,(chararray)SUBSTRING(record,200,202) AS BATCH_NUMBER,(chararray)SUBSTRING(record,202,208) AS PROCESSING_DATE;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdatadelimited GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, *;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY1}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA1}' );