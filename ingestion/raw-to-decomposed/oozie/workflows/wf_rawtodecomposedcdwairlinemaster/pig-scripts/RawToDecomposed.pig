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
rawdata = LOAD  '${SOURCE_DIRECTORY}'  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

rawdata_column = FOREACH rawdata  GENERATE (chararray)$0 AS airline_designator,(chararray)$1 AS airline_number,(chararray)$2 AS airline_three_letter_code,(chararray)$3 AS airline_host_country_name,(chararray)$4 AS airline_name,(chararray)$5 AS  airline_active_flag,(chararray)$6 AS effective_start_date,(chararray)$7 AS effective_end_date,(chararray)$8 AS last_modified_date,(chararray)$9 AS source_,(chararray)$10 AS duplicate_flag_indicator,(chararray)$11 AS address_line_1,(chararray)$12 AS address_line_2,(chararray)$13 AS airline_city,(chararray)$14 AS airline_state,(chararray)$15 AS  airline_postal_code,(chararray)$16 AS reservation_dept_teletype,(chararray)$17 AS reservations_contact_name,(chararray)$18 AS reservation_contact_title,(chararray)$19 AS reservation_contact_teletype,(chararray)$20 AS emergency_teletype,(chararray)$21 AS emergency_contact_name,(chararray)$22 AS emergency_contact_title,(chararray)$23 AS membership_flag_sita,(chararray)$24 AS membership_flag_arinc,(chararray)$25 AS  membership_flag_iata,(chararray)$26 AS type_operation_code,(chararray)$27 AS type_operation_description,(chararray)$28 AS accounting_code_secondary_flag,(chararray)$29 AS airline_prefix,(chararray)$30 AS airline_prefix_secondary_flag;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdata_column GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, * ;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );