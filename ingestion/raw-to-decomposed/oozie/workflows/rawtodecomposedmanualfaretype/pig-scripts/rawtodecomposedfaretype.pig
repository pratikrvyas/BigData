-- pig script from raw to decomposed with uuid and timestamp 
-- register required libraries 
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';


		
-- load data
rawdata = LOAD  '${SOURCE_DIRECTORY}'  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

rawdata_column = FOREACH rawdata GENERATE (chararray)$0 AS TKT_STOCK,(chararray)$1 AS TKT_STOCK_COND,(chararray)$2 AS DOC_NO,(chararray)$3 AS REV_NON_REV,(chararray)$4 AS FARE_TYPE,(chararray)$5 AS FARE_SUB_TYPE,(chararray)$6 AS Serial_NO,(chararray)$7 AS Specific_FBC_String_Checklist,(chararray)$8 AS on_after,(chararray)$9 AS Ticket_Iss_Eff_Date,(chararray)$10 AS on_before,(chararray)$11 AS Ticket_Iss_Disc_Date,(chararray)$12 AS and_Ticket_Designator,(chararray)$13 AS Ticket_Designator,(chararray)$14 AS and_Tour_Code_Pattern,(chararray)$15 AS Tour_Code_Pattern,(chararray)$16 AS and_Fares_Basis_Pattern,(chararray)$17 AS Fares_Basis_Pattern;


-- add uuid and timestamp
rawdatawithid = FOREACH rawdata_column GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, * ;

-- enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );