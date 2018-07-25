-- register required libraries
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

-- UDF for parsing email log htmlbody
REGISTER 'olci-pig-udfs-0.0.1.jar';
DEFINE parsehtml com.emirates.helix.udf.parser.ParseOLCIHtml;
DEFINE customsplit com.emirates.helix.udf.split.CustomStringSplit;

-- load the data
email_avrodata_egtc = LOAD '${SOURCE_DIRECTORY_EGTC}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_FILE}' );
email_avrodata_ntt = LOAD '${SOURCE_DIRECTORY_NTT}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_FILE}' );

email_avrodata= union email_avrodata_egtc, email_avrodata_ntt;

parsed_email_logs = FOREACH email_avrodata GENERATE FLATTEN(parsehtml(HTMLBODY)),customsplit(HTMLHEADER,'~',2) AS EMAIL,customsplit(PNR,'\\.',1) AS PNR, customsplit(DATECREATED,'\\.',1), EMAILTYPE;

parsed_email_logs_format = FOREACH parsed_email_logs GENERATE GIVEN_NAME AS GIVEN_NAME:chararray,FAMILY_NAME AS FAMILY_NAME:chararray,TICKET_NUMBER AS TICKET_NUMBER:chararray,FLIGHT_NUMBER AS FLIGHT_NUMBER:chararray,SUBSTRING(DEPARTURE_TIME,0,10) AS DEPARTURE_TIME:chararray,DEPARTURE_AIRPORT AS DEPARTURE_AIRPORT:chararray,ARRIVAL_AIRPORT AS ARRIVAL_AIRPORT:chararray,LOWER(EMAIL) AS EMAIL:chararray,PNR AS PNR:chararray,EMAILTYPE AS EMAIL_TYPE:chararray;

-- store into target directory
SET mapred.output.compress false;
STORE parsed_email_logs_format INTO '${TARGET_DIRECTORY}' USING JsonStorage();

