-- Pig script to move static file fare_basis_code_list from raw to decomposed with uuid and timestamp
-- Dev by : Jickson T George
-- Updated by : Jickson T George 2017/03/21
 
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

REGISTER  pigudf.jar;

--load data
rawdata = LOAD  '${SOURCE_DIRECTORY}'  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

rawdatawithschema = FOREACH rawdata  GENERATE (chararray)$0 AS id, (chararray)$1 AS fare_type, (chararray)$2 AS fbc_string_like, (chararray)$3 AS fare_category, (chararray)$4 AS usr_created, (chararray)$5 AS created, (chararray)$6 AS usr_changed, (chararray)$7 AS changed, (chararray)$8 AS pricing_check_reqd, (chararray)$9 AS ticket_start_date, (chararray)$10 AS ticket_end_date, (chararray)$11 AS descr;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdatawithschema GENERATE com.emirates.helix.pig.GenerateUUID('') as helix_uuid,  (chararray)ToUnixTime(CurrentTime()) as helix_timestamp, * ;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );