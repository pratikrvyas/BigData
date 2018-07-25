REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

--CBI_FRPS_OD_TRGT_REV metrics 

odtarget = LOAD '${S_D_OD_METRICS}'  USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${S_S_OD_METRICS}');

odtargetdata = foreach odtarget generate  BATCH_ID,(long)FNL_PAX,(bigdecimal) FNL_REV_BC;

groupedodtargetdata = GROUP odtargetdata BY BATCH_ID ;

odtargetrev_fnlpax = foreach groupedodtargetdata generate 'CBI_FRPS_OD_TRGT_REV' AS INTERFACE_ID,group AS BATCH_ID,'FNL_PAX' AS MEASURE_COLUMN,TRIM((chararray) SUM(odtargetdata.FNL_PAX)) AS MEASURE_VALUE;


odtargetrev_fnlrevbc1 = foreach groupedodtargetdata generate 'CBI_FRPS_OD_TRGT_REV' AS INTERFACE_ID,group AS BATCH_ID,'FNL_REV_BC' AS MEASURE_COLUMN, TRIM((chararray) SUM(odtargetdata.FNL_REV_BC))AS MEASURE_VALUE;


odtargetrev_fnlrevbc = foreach odtargetrev_fnlrevbc1 generate  INTERFACE_ID,BATCH_ID,MEASURE_COLUMN, SUBSTRING(MEASURE_VALUE, 0, INDEXOF(MEASURE_VALUE, '.', 0)+3) AS MEASURE_VALUE;


odtargetrev_metrics = UNION odtargetrev_fnlpax,odtargetrev_fnlrevbc;

--CBI_FRPS_POS_SCTR_TRGT_REV mterics

possctrtarget = LOAD '${S_D_POS_SCTR_REV}'  USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${S_S_POS_SCTR_REV}');

possctrtargetdata = foreach possctrtarget generate  BATCH_ID,(long)FNL_SCTR_PAX,(bigdecimal) FNL_REV_BC;

groupepossctrtargetdata = GROUP possctrtargetdata BY BATCH_ID ;

possctrtarget_fnlrevbc1 = foreach groupepossctrtargetdata generate 'CBI_FRPS_POS_SCTR_TRGT_REV' AS INTERFACE_ID,group AS BATCH_ID,'FNL_REV_BC' AS MEASURE_COLUMN, TRIM((chararray) SUM(possctrtargetdata.FNL_REV_BC)) AS MEASURE_VALUE;

possctrtarget_fnlrevbc = foreach possctrtarget_fnlrevbc1 generate  INTERFACE_ID,BATCH_ID,MEASURE_COLUMN, SUBSTRING(MEASURE_VALUE, 0, INDEXOF(MEASURE_VALUE, '.', 0)+3) AS MEASURE_VALUE;

possctrtarget_fnlsctrpax = foreach groupepossctrtargetdata generate 'CBI_FRPS_POS_SCTR_TRGT_REV' AS INTERFACE_ID,group AS BATCH_ID,'FNL_SCTR_PAX' AS MEASURE_COLUMN, TRIM((chararray) SUM(possctrtargetdata.FNL_SCTR_PAX)) AS MEASURE_VALUE;


possctrtarget_metrics = UNION possctrtarget_fnlsctrpax,possctrtarget_fnlrevbc;


--CBI_FRPS_RT_LEG_TRGT_REV mterics

rtlegtarget = LOAD '${S_D_LEG_TRGT_REV}'  USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${S_S_LEG_TRGT_REV}');

rtlegtargetdata = foreach rtlegtarget generate BATCH_ID,(long)FNL_LEG_PAX,(bigdecimal) FNL_REV_BC;

groupertlegtargetdata = GROUP rtlegtargetdata BY BATCH_ID ;

rtlegtarget_fnllegpax = foreach groupertlegtargetdata generate 'CBI_FRPS_RT_LEG_TRGT_REV' AS INTERFACE_ID,group AS BATCH_ID,'FNL_LEG_PAX' AS MEASURE_COLUMN, TRIM((chararray) SUM(rtlegtargetdata.FNL_LEG_PAX)) AS MEASURE_VALUE;
rtlegtarget_fnlrevbc1 = foreach groupertlegtargetdata generate 'CBI_FRPS_RT_LEG_TRGT_REV' AS INTERFACE_ID,group AS BATCH_ID,'FNL_REV_BC' AS MEASURE_COLUMN, TRIM((chararray) SUM(rtlegtargetdata.FNL_REV_BC)) AS MEASURE_VALUE;

rtlegtarget_fnlrevbc = foreach rtlegtarget_fnlrevbc1 generate  INTERFACE_ID,BATCH_ID,MEASURE_COLUMN, SUBSTRING(MEASURE_VALUE, 0, INDEXOF(MEASURE_VALUE, '.', 0)+3) AS MEASURE_VALUE;

rtlegtarget_metrics = UNION rtlegtarget_fnllegpax,rtlegtarget_fnlrevbc;

--CBI_FRPS_TRGT_EXHG_RTES mterics

targetexhg = LOAD '${S_D_TRGT_EXHG_RTES}'  USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${S_S_TRGT_EXHG_RTES}');

groupetargetexhgdata = GROUP targetexhg BY BATCH_ID ;

targetexhg_count = foreach groupetargetexhgdata generate 'CBI_FRPS_TRGT_EXHG_RTES' AS INTERFACE_ID,group AS BATCH_ID,'TOTAL' AS MEASURE_COLUMN, TRIM((chararray) COUNT(targetexhg.BATCH_ID)) AS MEASURE_VALUE;

--union all tables metrics

metrics = UNION odtargetrev_metrics,possctrtarget_metrics,rtlegtarget_metrics,targetexhg_count ;

valdata1 = LOAD '${S_D_VAL_DATA}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${S_S_VAL_DATA}');	

valdata = foreach valdata1 generate TRIM(INTERFACE_ID) AS INTERFACE_ID , TRIM(BATCH_ID) AS BATCH_ID, TRIM(MEASURE_COLUMN) AS MEASURE_COLUMN, (CASE INDEXOF(SRC_MEASURE_VALUE, '.', 0)  WHEN -1 THEN TRIM(SRC_MEASURE_VALUE) ELSE TRIM(SUBSTRING(SRC_MEASURE_VALUE, 0, INDEXOF(SRC_MEASURE_VALUE, '.', 0)+3)) END ) AS SRC_MEASURE_VALUE ;

joindata = join valdata by (INTERFACE_ID,BATCH_ID,MEASURE_COLUMN), metrics by (INTERFACE_ID,BATCH_ID,MEASURE_COLUMN);
 
filterdata_ok =  filter joindata by (chararray) valdata::SRC_MEASURE_VALUE == (chararray) metrics::MEASURE_VALUE ;

data_ok = foreach filterdata_ok generate metrics::INTERFACE_ID as INTERFACE_ID ,metrics::MEASURE_COLUMN as MEASURE_COLUMN, metrics::BATCH_ID as BATCH_ID,valdata::SRC_MEASURE_VALUE as SRC_MEASURE_VALUE,metrics::MEASURE_VALUE as TRG_MEASURE_VALUE, 'OK' as VALIDATION_STATUS ;

filterdata_notok =  filter joindata by (chararray) valdata::SRC_MEASURE_VALUE != (chararray) metrics::MEASURE_VALUE ;

data_notok = foreach filterdata_notok generate metrics::INTERFACE_ID as INTERFACE_ID , metrics::MEASURE_COLUMN as MEASURE_COLUMN, metrics::BATCH_ID as BATCH_ID,valdata::SRC_MEASURE_VALUE as SRC_MEASURE_VALUE,metrics::MEASURE_VALUE as TRG_MEASURE_VALUE, 'ERROR' as VALIDATION_STATUS ;

final_data =  UNION data_ok,data_notok;

STORE final_data INTO '${TARGET_DIRECTORY}' USING PigStorage(',');