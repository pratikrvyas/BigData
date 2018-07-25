-- register required libraries
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

-- UDF for parsing email log htmlbody
REGISTER 'olci-pig-udfs-0.0.1.jar';
REGISTER 'helix-pig-udfs-0.0.1.jar';
DEFINE customsplit com.emirates.helix.udf.split.CustomStringSplit;
DEFINE dateformat com.emirates.helix.pig.date.DateFormatter;

sms_avrodata_egtc = LOAD '${SOURCE_DIRECTORY_EGTC}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_FILE}' );
sms_avrodata_ntt = LOAD '${SOURCE_DIRECTORY_NTT}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_FILE}' );

sms_avrodata= union sms_avrodata_egtc, sms_avrodata_ntt;

parsed_sms_logs = FOREACH sms_avrodata GENERATE customsplit(PNR,'\\.',1) AS PNR, ORIGIN, DESTINATION, MOBILE_NUMBER, BOOKED_DATE, customsplit(SMS_MESSAGE,' ',1) AS PAX_NAME, (customsplit(SMS_MESSAGE,' ',2) MATCHES 'EK.*'? customsplit(SMS_MESSAGE,' ',2) : customsplit(SMS_MESSAGE,' ',3)) AS FLIGHT_NUMBER, (customsplit(SMS_MESSAGE,' ',2) MATCHES 'EK.*'? LOWER(customsplit(SMS_MESSAGE,' ',4)) : LOWER(customsplit(SMS_MESSAGE,' ',5))) AS DEPARTURE_DATE, (customsplit(SMS_MESSAGE,' ',2) MATCHES 'EK.*'? customsplit(SMS_MESSAGE,' ',5) : customsplit(SMS_MESSAGE,' ',6)) AS DEPARTURE_TIME, CABIN_CLASS, ACITON_DATE AS ACTION_DATE;

parsed_sms_logs_format = FOREACH parsed_sms_logs GENERATE PNR AS PNR:chararray, ORIGIN as ORGIN:chararray, DESTINATION as DESTINATION:chararray, MOBILE_NUMBER as MOBILE_NUMBER:chararray, BOOKED_DATE as BOOKED_DATE:chararray, PAX_NAME as PAX_NAME:chararray, REPLACE(FLIGHT_NUMBER,'EK','0') as FLIGHT_NUMBER:chararray, dateformat(DEPARTURE_DATE,'ddMMMyy','yyyy-MM-dd') as DEPARTURE_DATE:chararray, DEPARTURE_TIME as DEPARTURE_TIME:chararray, CABIN_CLASS AS CABIN_CLASS:chararray, ACTION_DATE AS ACTION_DATE:chararray;

-- store into target directory
SET mapred.output.compress false;
STORE parsed_sms_logs_format INTO '${TARGET_DIRECTORY}' USING JsonStorage();

