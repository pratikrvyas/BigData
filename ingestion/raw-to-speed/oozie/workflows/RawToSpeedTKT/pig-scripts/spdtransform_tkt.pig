-- register required libraries
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

-- load the data
tkt_master = LOAD '${SOURCE_DIRECTORY_TKT_MASTER}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_TKT_MASTER}' );
tkt_flt_cpn = LOAD '${SOURCE_DIRECTORY_TKT_FLIGHT_CPN}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_TKT_FLIGHT_CPN}' );
tkt_pax_fare = LOAD '${SOURCE_DIRECTORY_TKT_PAX_FARE}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_TKT_PAX_FARE}' );
tkt_operating_carrier = LOAD '${SOURCE_DIRECTORY_TKT_OPERATING_CARRIER}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_TKT_OPERATING_CARRIER}' );

-- project th required columns
cols_tkt_master = FOREACH tkt_master GENERATE TKT_ID, TICKET_NUMBER, PNR_REC_LOCATOR, PNR_CREATION_DATE, CREATED_DATE;
cols_tkt_flt_cpn = FOREACH tkt_flt_cpn GENERATE TKT_ID, TKT_CPN_ID,CPN_EXVOID_NO, TICKET_NUMBER, COUPON_NUMBER, COUPON_STATUS_INDICATOR, COUPON_STATUS_DESCRIPTION, PNR_REC_LOCATOR, PNR_CREATION_DATE, AIRLINE_CODE, FLIGHT_NUMBER, RBD_CLASS_CODE, CLASS_OF_SERVICE, DEPARTURE_DATE, DEPARTURE_TIME, ARRIVAL_DATE, ARRIVAL_TIME, BOARD_POINT, OFFPOINT, FARE_BASIS_DATA, FARE_BASIS_DATA_SIZE,  FARE_BRAND_IND, INDUSTRY_DISCOUNT_DATA, CREATED_DATE;
cols_tkt_pax_fare = FOREACH tkt_pax_fare GENERATE TKT_ID, PAX_NAME, CREATED_DATE;
cols_tkt_operating_carrier = FOREACH tkt_operating_carrier GENERATE TKT_ID, CPN_EXVOID_NO, MARKETING_CARRIER_CODE, MARKETING_CARRIER_FLT_NUMBER, OPERATING_CARRIER_CODE, OPERATING_CARRIER_FLT_NUM, CREATED_DATE;

-- dedupe step 1 - get max created date for each ticket_master
tkt_master_distinct = DISTINCT cols_tkt_master;
tkt_master_copy = FOREACH tkt_master_distinct GENERATE *;
tkt_master_key = FOREACH tkt_master_distinct GENERATE TKT_ID, CREATED_DATE;
tkt_master_grouped = GROUP tkt_master_key BY ( TKT_ID );
tkt_master_latest = FOREACH tkt_master_grouped GENERATE group, MAX(tkt_master_key.CREATED_DATE) as max_date;
tkt_master_join = COGROUP tkt_master_copy BY ( TKT_ID, CREATED_DATE ), tkt_master_latest BY ( group, max_date );
tkt_master_filter = FILTER tkt_master_join BY NOT IsEmpty( tkt_master_latest );
tkt_master_final = FOREACH tkt_master_filter GENERATE FLATTEN( tkt_master_copy );


-- dedupe step 1 - get max created date for each ticket_coupon
tkt_flt_cpn_distinct = DISTINCT cols_tkt_flt_cpn;
tkt_flt_cpn_copy = FOREACH tkt_flt_cpn_distinct GENERATE *;
tkt_flt_cpn_key = FOREACH tkt_flt_cpn_distinct GENERATE TKT_ID, CPN_EXVOID_NO, CREATED_DATE;
tkt_flt_cpn_grouped = GROUP tkt_flt_cpn_key BY ( TKT_ID, CPN_EXVOID_NO );
tkt_flt_cpn_latest = FOREACH tkt_flt_cpn_grouped GENERATE group, MAX(tkt_flt_cpn_key.CREATED_DATE) as max_date;
tkt_flt_cpn_join = COGROUP tkt_flt_cpn_copy BY ( TKT_ID, CPN_EXVOID_NO, CREATED_DATE ), tkt_flt_cpn_latest BY ( group.TKT_ID, group.CPN_EXVOID_NO, max_date );
tkt_flt_cpn_filter = FILTER tkt_flt_cpn_join BY NOT IsEmpty( tkt_flt_cpn_latest );
tkt_flt_cpn_final = FOREACH tkt_flt_cpn_filter GENERATE FLATTEN( tkt_flt_cpn_copy );

-- dedupe step 1 - get max created date for each pax fare
tkt_pax_fare_distinct = DISTINCT cols_tkt_pax_fare;
tkt_pax_fare_copy = FOREACH tkt_pax_fare_distinct GENERATE *;
tkt_pax_fare_key = FOREACH tkt_pax_fare_distinct GENERATE TKT_ID, CREATED_DATE;
tkt_pax_fare_grouped = GROUP tkt_pax_fare_key BY ( TKT_ID );
tkt_pax_fare_latest = FOREACH tkt_pax_fare_grouped GENERATE group, MAX(tkt_pax_fare_key.CREATED_DATE) as max_date;
tkt_pax_fare_join = COGROUP tkt_pax_fare_copy BY ( TKT_ID, CREATED_DATE ), tkt_pax_fare_latest BY ( group, max_date );
tkt_pax_fare_filter = FILTER tkt_pax_fare_join BY NOT IsEmpty( tkt_pax_fare_latest );
tkt_pax_fare_final = FOREACH tkt_pax_fare_filter GENERATE FLATTEN( tkt_pax_fare_copy );

-- dedupe step 1 - get max created date for each ticket operating carrier
tkt_operating_carrier_distinct = DISTINCT cols_tkt_operating_carrier;
tkt_operating_carrier_copy = FOREACH tkt_operating_carrier_distinct GENERATE *;
tkt_operating_carrier_key = FOREACH tkt_operating_carrier_distinct GENERATE TKT_ID, CPN_EXVOID_NO, CREATED_DATE;
tkt_operating_carrier_grouped = GROUP tkt_operating_carrier_key BY ( TKT_ID, CPN_EXVOID_NO );
tkt_operating_carrier_latest = FOREACH tkt_operating_carrier_grouped GENERATE group, MAX(tkt_operating_carrier_key.CREATED_DATE) as max_date;
tkt_operating_carrier_join = COGROUP tkt_operating_carrier_copy BY ( TKT_ID, CPN_EXVOID_NO, CREATED_DATE ), tkt_operating_carrier_latest BY ( group.TKT_ID, group.CPN_EXVOID_NO, max_date );
tkt_operating_carrier_filter = FILTER tkt_operating_carrier_join BY NOT IsEmpty( tkt_operating_carrier_latest );
tkt_operating_carrier_final = FOREACH tkt_operating_carrier_filter GENERATE FLATTEN( tkt_operating_carrier_copy );

-- join the data together
join_step1 = JOIN tkt_master_final BY ( TKT_ID), tkt_flt_cpn_final BY ( TKT_ID), tkt_pax_fare_final BY ( TKT_ID );
join_step2 = JOIN join_step1 BY(tkt_flt_cpn_final::tkt_flt_cpn_copy::TKT_ID, tkt_flt_cpn_final::tkt_flt_cpn_copy::CPN_EXVOID_NO), tkt_operating_carrier_final BY(TKT_ID, CPN_EXVOID_NO);

-- get the final output fields 
final = FOREACH join_step2 GENERATE
			(tkt_flt_cpn_final::tkt_flt_cpn_copy::PNR_REC_LOCATOR is null OR tkt_flt_cpn_final::tkt_flt_cpn_copy::PNR_REC_LOCATOR == '' ? tkt_master_final::tkt_master_copy::PNR_REC_LOCATOR : tkt_flt_cpn_final::tkt_flt_cpn_copy::PNR_REC_LOCATOR) AS pnr_rec_locator,
			(tkt_flt_cpn_final::tkt_flt_cpn_copy::PNR_CREATION_DATE is null OR tkt_flt_cpn_final::tkt_flt_cpn_copy::PNR_CREATION_DATE  == '' ? SUBSTRING(tkt_master_final::tkt_master_copy::PNR_CREATION_DATE,0,10) : SUBSTRING(tkt_flt_cpn_final::tkt_flt_cpn_copy::PNR_CREATION_DATE,0,10)) AS pnr_creation_date,
			tkt_pax_fare_final::tkt_pax_fare_copy::PAX_NAME AS pax_name,
			tkt_operating_carrier_final::tkt_operating_carrier_copy::OPERATING_CARRIER_CODE AS operating_carrier_code,
			tkt_operating_carrier_final::tkt_operating_carrier_copy::OPERATING_CARRIER_FLT_NUM AS operating_carrier_flt_num,
			tkt_operating_carrier_final::tkt_operating_carrier_copy::MARKETING_CARRIER_CODE AS airline_code, -- marketing_carrier_code
			tkt_operating_carrier_final::tkt_operating_carrier_copy::MARKETING_CARRIER_FLT_NUMBER AS flight_number, -- marketing_carrier_flt_number,
			SUBSTRING(tkt_flt_cpn_final::tkt_flt_cpn_copy::DEPARTURE_DATE,0,10) AS departure_date,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::DEPARTURE_TIME AS departure_time,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::BOARD_POINT AS board_point,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::OFFPOINT AS  off_point,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::CLASS_OF_SERVICE AS class_of_service,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::CPN_EXVOID_NO AS cpn_exvoid_no,
            tkt_flt_cpn_final::tkt_flt_cpn_copy::COUPON_NUMBER AS coupon_number,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::COUPON_STATUS_DESCRIPTION AS coupon_status_description,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::COUPON_STATUS_INDICATOR AS coupon_status_indicator,
			tkt_master_final::tkt_master_copy::TICKET_NUMBER AS master_ticket_number,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::TICKET_NUMBER AS ticket_number,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::FARE_BASIS_DATA AS fare_basis_data,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::FARE_BASIS_DATA_SIZE AS fare_basis_data_size,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::FARE_BRAND_IND AS fare_brand_ind,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::INDUSTRY_DISCOUNT_DATA AS industry_discount_data,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::RBD_CLASS_CODE AS rbd_class_code,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::TKT_CPN_ID AS tkt_cpn_id,
			tkt_flt_cpn_final::tkt_flt_cpn_copy::TKT_ID AS tkt_id
			;

-- store into target directory
SET mapred.output.compress false;
STORE final INTO '${TARGET_DIRECTORY}' USING JsonStorage();