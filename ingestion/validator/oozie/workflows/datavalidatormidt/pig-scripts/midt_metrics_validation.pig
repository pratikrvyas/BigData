REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

--midt pnr metrics

midtpnr = LOAD '${S_D_PNR}'  USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${S_S_PNR}'); 

groupedpnrdata = GROUP midtpnr BY (BATCH_NUMBER,PROCESSING_DATE) ;

midtpnr_metrics= foreach groupedpnrdata generate 'MIDT_PNR' AS INTERFACE_ID,FLATTEN(group) as (BATCH_NUMBER, PROCESSING_DATE),'COUNT' AS MEASURE_COLUMN,TRIM((chararray) COUNT(midtpnr)) AS MEASURE_VALUE;

midtagent = LOAD '${S_D_AGENT}'  USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${S_S_AGENT}'); 

groupedagentdata = GROUP midtagent BY (BATCH_NUMBER,PROCESSING_DATE) ;

midtagent_metrics = foreach groupedagentdata generate 'MIDT_AGENT_MASTER' AS INTERFACE_ID,FLATTEN(group) as (BATCH_NUMBER, PROCESSING_DATE),'COUNT' AS MEASURE_COLUMN,TRIM((chararray) COUNT(midtagent)) AS MEASURE_VALUE;

union_data_metrics = UNION midtpnr_metrics,midtagent_metrics;

midtpnrcontrol = LOAD '${S_D_PNR_CONTROL}'  USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${S_S_PNR_CONTROL}'); 

midtpnrcontrol_metrics = foreach midtpnrcontrol generate 'MIDT_PNR' AS INTERFACE_ID,BATCH_NUMBER,PROCESSING_DATE,'COUNT' AS MEASURE_COLUMN,SUBSTRING(NUMBER_OF_RECORDS, INDEXOF(NUMBER_OF_RECORDS, '=', 0)+1,(int) SIZE(NUMBER_OF_RECORDS)) AS MEASURE_VALUE;

midtagentcontrol = LOAD '${S_D_AGENT_CONTROL}'  USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${S_S_AGENT_CONTROL}'); 

midtagentcontrol_metrics = foreach midtagentcontrol generate 'MIDT_AGENT_MASTER' AS INTERFACE_ID,BATCH_NUMBER,PROCESSING_DATE,'COUNT' AS MEASURE_COLUMN,SUBSTRING(NUMBER_OF_RECORDS, INDEXOF(NUMBER_OF_RECORDS, '=', 0)+1,(int) SIZE(NUMBER_OF_RECORDS)) AS MEASURE_VALUE;

union_control_metrics = UNION midtpnrcontrol_metrics,midtagentcontrol_metrics;

joindata = join union_data_metrics by (INTERFACE_ID,BATCH_NUMBER,PROCESSING_DATE) LEFT OUTER, union_control_metrics by (INTERFACE_ID,BATCH_NUMBER,PROCESSING_DATE);

filterdata_ok =  filter joindata by (chararray) REGEX_EXTRACT(TRIM(union_data_metrics::MEASURE_VALUE),'0*(\\d+)?', 1) == (chararray) REGEX_EXTRACT(TRIM(union_control_metrics::MEASURE_VALUE),'0*(\\d+)?', 1) ;

data_ok = foreach filterdata_ok generate union_data_metrics::INTERFACE_ID as INTERFACE_ID ,union_data_metrics::MEASURE_COLUMN as MEASURE_COLUMN, union_data_metrics::BATCH_NUMBER as BATCH_NUMBER,union_data_metrics::PROCESSING_DATE as PROCESSING_DATE,union_data_metrics::MEASURE_VALUE as DATA_FILE_MEASURE_VALUE,TRIM(union_control_metrics::MEASURE_VALUE) as CONFROL_FILE_MEASURE_VALUE, 'OK' as VALIDATION_STATUS ;

filterdata_notok =  filter joindata by (chararray) (TRIM(union_data_metrics::MEASURE_VALUE) is not null? REGEX_EXTRACT(TRIM(union_data_metrics::MEASURE_VALUE),'0*(\\d+)?', 1) : 'yyyyyyy') != (chararray) (TRIM(union_control_metrics::MEASURE_VALUE) is not null? REGEX_EXTRACT(TRIM(union_control_metrics::MEASURE_VALUE),'0*(\\d+)?', 1) : 'xxxxxxxx') ;

data_notok = foreach filterdata_notok generate union_data_metrics::INTERFACE_ID as INTERFACE_ID ,union_data_metrics::MEASURE_COLUMN as MEASURE_COLUMN, union_data_metrics::BATCH_NUMBER as BATCH_NUMBER,union_data_metrics::PROCESSING_DATE as PROCESSING_DATE,union_data_metrics::MEASURE_VALUE as DATA_FILE_MEASURE_VALUE,TRIM(union_control_metrics::MEASURE_VALUE) as CONFROL_FILE_MEASURE_VALUE, 'ERROR' as VALIDATION_STATUS ;

final_data =  UNION data_ok,data_notok;

STORE final_data INTO '${TARGET_DIRECTORY}' USING PigStorage(',');