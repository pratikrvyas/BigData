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
rawdata = LOAD '${SOURCE_DIRECTORY1}' AS (record:chararray); 

rawdatadelimited = foreach rawdata generate (chararray)SUBSTRING(record,0,4) AS MONTH,(chararray)SUBSTRING(record,4,7) AS AIRLINE_CODE,(chararray)SUBSTRING(record,7,14) AS AGENT_CODE,(chararray)SUBSTRING(record,14,15) AS STAT,(chararray)SUBSTRING(record,15,18) AS ORIGIN_AIRPORT,(chararray)SUBSTRING(record,18,21) AS DESTINATION_AIRPORT,(chararray)SUBSTRING(record,21,22) AS STATUS,(chararray)SUBSTRING(record,22,23) AS CLASS_CATEGORY,(chararray)SUBSTRING(record,23,27) AS FLIGHT_MONTH,(chararray)SUBSTRING(record,27,43) AS GROSS_INDUSTRY_AMOUNT,(chararray)SUBSTRING(record,43,59) AS GROSS_AIRLINE_AMOUNT,(chararray)SUBSTRING(record,59,75) AS NET_INDUSTRY_AMOUNT,(chararray)SUBSTRING(record,75,91) AS NET_AIRLINE_AMOUNT,(chararray)SUBSTRING(record,91,98) AS INDUSTRY_DOCUMENTS,(chararray)SUBSTRING(record,98,105) AS AIRLINE_DOCUMENTS,(chararray)SUBSTRING(record,105,109) AS CUTP,(chararray)SUBSTRING(record,109,111) AS COUNTRY,(chararray)SUBSTRING(record,111,117) AS PROCESSING_MONTH;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdatadelimited GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, *;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;

 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY1}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA1}' );