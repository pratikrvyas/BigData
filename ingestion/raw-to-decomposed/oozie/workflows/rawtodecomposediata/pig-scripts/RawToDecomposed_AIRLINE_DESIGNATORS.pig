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

rawdatadelimited = foreach rawdata generate (chararray)SUBSTRING(record,0,40) AS AIRLINE_NAME_LINE1 ,(chararray)SUBSTRING(record,40, 80) AS AIRLINE_NAME_LINE2 ,(chararray)SUBSTRING(record,80,84) AS ACCOUNTING_CODE,(chararray)SUBSTRING(record,84,87) AS THREE_LETTER_CODE,(chararray)SUBSTRING(record,87,89) AS TWO_CHARACTER_CODE,(chararray)SUBSTRING(record,89,90) AS DUPLICATE_FLAG_INDICATOR,(chararray)SUBSTRING(record,90,130) AS ADDRESS_LINE1,(chararray)SUBSTRING(record,130,170) AS ADDRESS_LINE2,(chararray)SUBSTRING(record,170,195) AS AIRLINE_CITY,(chararray)SUBSTRING(record,195,215) AS AIRLINE_STATE,(chararray)SUBSTRING(record,215,259) AS AIRLINE_COUNTRY,(chararray)SUBSTRING(record,259,269) AS AIRLINE_POSTAL_CODE,(chararray)SUBSTRING(record,269,277) AS RESERVATION_DEPT_TELETYPE,(chararray)SUBSTRING(record,277,297) AS RESERVATIONS_CONTACT_NAME,(chararray)SUBSTRING(record,297,317) AS RESERVATION_CONTACT_TITLE,(chararray)SUBSTRING(record,317,325) AS RESERVATION_CONTACT_TELETYPE,(chararray)SUBSTRING(record,325,333) AS EMERGENCY_TELETYPE,(chararray)SUBSTRING(record,333,353) AS EMERGENCY_CONTACT_NAME,(chararray)SUBSTRING(record,353,373) AS EMERGENCY_CONTACT_TITLE,(chararray)SUBSTRING(record,373,374) AS MEMBERSHIP_FLAG_SITA,(chararray)SUBSTRING(record,374,375) AS MEMBERSHIP_FLAG_ARINC,(chararray)SUBSTRING(record,375,376) AS MEMBERSHIP_FLAG_IATA,(chararray)SUBSTRING(record,376,377) AS MEMBERSHIP_FLAG_ATA,(chararray)SUBSTRING(record,377,378) AS TYPE_OPRATION_CODE,(chararray)SUBSTRING(record,378,379) AS ACCOUNTING_CODE_SECONDARY_FLAG,(chararray)SUBSTRING(record,379,382) AS AIRLINE_PREFIX,(chararray)SUBSTRING(record,382,383) AS AIRLINE_PREFIX_SECONDARY_FLAG;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdatadelimited GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, *;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );
		