-- register required libraries
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/datafu.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER 'helix-pig-udfs-0.0.1.jar';

-- define custom functions
define BagConcat datafu.pig.bags.BagConcat();
define extract_pnremail com.emirates.helix.pig.emcg.EmailExtractor;

-- load the data
input_pnr = LOAD '${SOURCE_DIRECTORY_PASSENGER_NAME_RECORDS}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${SOURCE_SCHEMA_PASSENGER_NAME_RECORDS}' );
input_pax = LOAD '${SOURCE_DIRECTORY_PAX}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${SOURCE_SCHEMA_PAX}' );
input_itns = LOAD '${SOURCE_DIRECTORY_ITNS}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${SOURCE_SCHEMA_ITNS}' );
input_hfx_gfxs = LOAD '${SOURCE_DIRECTORY_HFX_GFXS}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${SOURCE_SCHEMA_HFX_GFXS}' );
input_others = LOAD '${SOURCE_DIRECTORY_OTHERS}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${SOURCE_SCHEMA_OTHERS}' );

-- project th required columns
cols_pnr = FOREACH input_pnr GENERATE REC_LOCATOR, PNR_CREATION_DATE, ID, VERSION_NO, CREATOR_CITY, POS_CRS_SYSTEM_DESIGNATOR, POS_COUNTRY_CODE, GROUP_BOOKING_IND;
cols_pax = FOREACH input_pax GENERATE PNR_ID, VERSION_NO, PAX_ID, PNR_PAX_NAME, SUFFIX;
cols_itns = FOREACH input_itns GENERATE PNR_ID, VERSION_NO, BOARD_POINT, OFF_POINT, BOOKING_STATUS, DEPARTURE_DATE, DEPARTURE_TIME, HOST_SEGMENT_NUMBER, OAL_SEGMENT_TYPE, RESERVATION_CLASS_CODE, SERVICE_CLASS_CODE, OAL_AIRLINE_DESIGNATOR, AIRLINE_DESIGNATOR, OAL_FLIGHT_NUMBER, FLIGHT_NUMBER;
cols_hfx_gfxs = FOREACH input_hfx_gfxs GENERATE PNR_ID, VERSION_NO, PNR_PAX_ID, SERVICE_TYPE, ((SERVICE_CODE is null or REPLACE( SERVICE_CODE, ' ', '' ) == '') ? SUBSTRING(REPLACE(VARIABLE_DATA,' ',''),0,4) : SERVICE_CODE) AS SERVICE_CODE, VARIABLE_DATA, FFP_SUFFIX, FFP_NUMBER;
cols_others = FOREACH input_others GENERATE PNR_ID, VERSION_NO, OTHER_CODE, VARIABLE_DATA;

-- step 1 - get max id for each pnr
inputfull_step1 = FOREACH cols_pnr GENERATE *;
inputproj_step1 = FOREACH cols_pnr GENERATE REC_LOCATOR, PNR_CREATION_DATE, ID;
grouped_step1 = GROUP inputproj_step1 BY ( REC_LOCATOR, PNR_CREATION_DATE );
maxmin_step1 = FOREACH grouped_step1 GENERATE group, MAX(inputproj_step1.ID) as max_id;
joined_step1 = COGROUP inputfull_step1 BY ( REC_LOCATOR, PNR_CREATION_DATE, ID ), maxmin_step1 BY ( group.REC_LOCATOR, group.PNR_CREATION_DATE, max_id );
filtered_step1 = FILTER joined_step1 BY NOT IsEmpty( maxmin_step1 );
output_step1 = FOREACH filtered_step1 GENERATE FLATTEN( inputfull_step1 );

-- step 2 - get max version
inputfull_step2 = FOREACH output_step1 GENERATE *;
inputproj_step2 = FOREACH output_step1 GENERATE REC_LOCATOR, PNR_CREATION_DATE, VERSION_NO;
grouped_step2 = GROUP inputproj_step2 BY ( REC_LOCATOR, PNR_CREATION_DATE );
maxmin_step2 = FOREACH grouped_step2 GENERATE group, MAX(inputproj_step2.VERSION_NO) as max_version;
joined_step2 = COGROUP inputfull_step2 BY ( REC_LOCATOR, PNR_CREATION_DATE, VERSION_NO ), maxmin_step2 BY ( group.REC_LOCATOR, group.PNR_CREATION_DATE, max_version );
filtered_step2 = FILTER joined_step2 BY NOT IsEmpty( maxmin_step2 );
output_step2 = FOREACH filtered_step2 GENERATE FLATTEN( inputfull_step2 );

-- this is the correct ID and VERSION for each PNR
calc_pnr = FOREACH output_step2 GENERATE *;

-- calculated fields for PNR_PAX
calc_pax = FOREACH cols_pax GENERATE PNR_ID, VERSION_NO, PAX_ID, PNR_PAX_NAME;

-- calculated fields for PNR_ITNS
calc_itns = FOREACH cols_itns GENERATE PNR_ID, VERSION_NO, BOARD_POINT, OFF_POINT, BOOKING_STATUS, DEPARTURE_DATE,  DEPARTURE_TIME, HOST_SEGMENT_NUMBER, 
	RESERVATION_CLASS_CODE AS CABIN_CLASS,
	SERVICE_CLASS_CODE,
	( OAL_SEGMENT_TYPE IS NULL ? AIRLINE_DESIGNATOR : ( OAL_SEGMENT_TYPE == 'O' ? OAL_AIRLINE_DESIGNATOR : ( OAL_SEGMENT_TYPE == 'M' ? AIRLINE_DESIGNATOR : '' ))) AS OPERATING_AIRLINE,
	( OAL_SEGMENT_TYPE IS NULL ? AIRLINE_DESIGNATOR : ( OAL_SEGMENT_TYPE == 'O' ? AIRLINE_DESIGNATOR : ( OAL_SEGMENT_TYPE == 'M' ? OAL_AIRLINE_DESIGNATOR : '' ))) AS MARKETING_AIRLINE,
	( OAL_SEGMENT_TYPE IS NULL ? FLIGHT_NUMBER : ( OAL_SEGMENT_TYPE == 'O' ? OAL_FLIGHT_NUMBER : ( OAL_SEGMENT_TYPE == 'M' ? FLIGHT_NUMBER : '' ))) AS OPERATING_FLIGHT_NUMBER,
	( OAL_SEGMENT_TYPE IS NULL ? FLIGHT_NUMBER : ( OAL_SEGMENT_TYPE == 'O' ? FLIGHT_NUMBER : ( OAL_SEGMENT_TYPE == 'M' ? OAL_FLIGHT_NUMBER : '' ))) AS MARKETING_FLIGHT_NUMBER;

-- calculated fields for PNR_HFX_GFXS - emails
filt_emails = FILTER cols_hfx_gfxs BY ( REPLACE( SERVICE_CODE, ' ', '' ) == 'CTCE' );
calc_emails1 = FOREACH filt_emails GENERATE PNR_ID, VERSION_NO, PNR_PAX_ID,
	( REPLACE( SERVICE_CODE, ' ', '' ) == 'CTCE' ? ( SERVICE_TYPE == 'OSI' ? 
		[ 'email_address', extract_pnremail(VARIABLE_DATA,'OSI'), 'source', 'PNR' ] : 
		[ 'email_address', extract_pnremail(VARIABLE_DATA,'SSR'), 'source', 'PNR' ] ) : 
	null ) AS email;
calc_emails2 = FILTER calc_emails1 BY email#'email_address' IS NOT NULL;

grp_emails = GROUP calc_emails2 BY( PNR_ID, VERSION_NO, PNR_PAX_ID );
grpd_emails = FOREACH grp_emails {
	email1 = FILTER calc_emails2 BY email IS NOT NULL;
	email2 = email1.email;
	email3 = DISTINCT email2;
	GENERATE group.PNR_ID AS PNR_ID, group.VERSION_NO AS VERSION_NO, group.PNR_PAX_ID AS PNR_PAX_ID, email3 AS PNR_EMAILS;
};
grpd_emails_pnr_level = FILTER grpd_emails BY (PNR_PAX_ID IS NULL or PNR_PAX_ID == '');

-- calculated fields for PNR_HFX_GFXS - phones
filt_phones = FILTER cols_hfx_gfxs BY ( SERVICE_CODE MATCHES 'CTC ?[B|H|M|P]' );
calc_phones1 = FOREACH filt_phones GENERATE PNR_ID, VERSION_NO, PNR_PAX_ID,
	( SERVICE_CODE == 'CTCB' ? ( (int)SIZE( REGEX_EXTRACT( REPLACE( VARIABLE_DATA, '[\\s-+]' , '' ), '[\\d]{5,}', 0 ) ) >= 6 ? [ 'phone_number', REPLACE( REGEX_EXTRACT( REPLACE( VARIABLE_DATA, '-' ,'' ), '[\\d\\s-+]{5,}', 0 ), '[\\s-+]' ,'' ), 'phone_type', 'WORKPHONE', 'source', 'PNR', 'validated', ( INDEXOF( VARIABLE_DATA, 'PREF' ) > -1 ? 'Y' : ( INDEXOF( VARIABLE_DATA, 'VALID' ) > -1 ? 'Y' : 'N' )) ] : NULL ) : 
	( SERVICE_CODE == 'CTCH' ? ( (int)SIZE( REGEX_EXTRACT( REPLACE( VARIABLE_DATA, '[\\s-+]' , '' ), '[\\d]{5,}', 0 ) ) >= 6 ? [ 'phone_number', REPLACE( REGEX_EXTRACT( REPLACE( VARIABLE_DATA, '-' ,'' ), '[\\d\\s-+]{5,}', 0 ), '[\\s-+]' ,'' ), 'phone_type', 'HOMEPHONE', 'source', 'PNR', 'validated', ( INDEXOF( VARIABLE_DATA, 'PREF' ) > -1 ? 'Y' : ( INDEXOF( VARIABLE_DATA, 'VALID' ) > -1 ? 'Y' : 'N' )) ] : NULL ) : 
	( SERVICE_CODE == 'CTCM' ? ( (int)SIZE( REGEX_EXTRACT( REPLACE( VARIABLE_DATA, '[\\s-+]' , '' ), '[\\d]{5,}', 0 ) ) >= 6 ? [ 'phone_number', REPLACE( REGEX_EXTRACT( REPLACE( VARIABLE_DATA, '-' ,'' ), '[\\d\\s-+]{5,}', 0 ), '[\\s-+]' ,'' ), 'phone_type', 'MOBILEPHONE', 'source', 'PNR', 'validated', ( INDEXOF( VARIABLE_DATA, 'PREF' ) > -1 ? 'Y' : ( INDEXOF( VARIABLE_DATA, 'VALID' ) > -1 ? 'Y' : 'N' )) ] : NULL ) : 
	( SERVICE_CODE == 'CTCP' ? ( (int)SIZE( REGEX_EXTRACT( REPLACE( VARIABLE_DATA, '[\\s-+]' , '' ), '[\\d]{5,}', 0 ) ) >= 6 ? [ 'phone_number', REPLACE( REGEX_EXTRACT( REPLACE( VARIABLE_DATA, '-' ,'' ), '[\\d\\s-+]{5,}', 0 ), '[\\s-+]' ,'' ), 'phone_type', 'OTHERPHONE', 'source', 'PNR', 'validated', ( INDEXOF( VARIABLE_DATA, 'PREF' ) > -1 ? 'Y' : ( INDEXOF( VARIABLE_DATA, 'VALID' ) > -1 ? 'Y' : 'N' )) ] : NULL ) : 
		NULL )))) AS phone;
calc_phones2 = FILTER calc_phones1 BY phone#'phone_number' IS NOT NULL;

grp_phones = GROUP calc_phones2 BY( PNR_ID, VERSION_NO, PNR_PAX_ID );
grpd_phones = FOREACH grp_phones {
	phone1 = FILTER calc_phones2 BY phone IS NOT NULL;
	phone2 = phone1.phone;
	phone3 = DISTINCT phone2;
	GENERATE group.PNR_ID AS PNR_ID, group.VERSION_NO AS VERSION_NO, group.PNR_PAX_ID AS PNR_PAX_ID, phone3 AS PNR_PHONES;
};
grpd_phones_pnr_level = FILTER grpd_phones BY (PNR_PAX_ID IS NULL or PNR_PAX_ID == '');

-- calculated fields for PNR_HFX_GFXS - OAL FFP
filt_oalffps = FILTER cols_hfx_gfxs BY ( SERVICE_CODE MATCHES 'FQT[R|V]' AND FFP_NUMBER IS NOT NULL AND FFP_SUFFIX !='EK' );
calc_oalffps = FOREACH filt_oalffps GENERATE PNR_ID, VERSION_NO, PNR_PAX_ID, FFP_SUFFIX, FFP_NUMBER;

-- only one OAL FFP is returned for each PNR_ID, VERSION_NO, PNR_PAX_ID
grp_oalffps = GROUP calc_oalffps BY( PNR_ID, VERSION_NO, PNR_PAX_ID );
grpd_oalffps = FOREACH grp_oalffps {
	sort_ffp = ORDER calc_oalffps BY FFP_SUFFIX, FFP_NUMBER;
	first_ffp = LIMIT sort_ffp 1;
	GENERATE group, FLATTEN( first_ffp );
};
dist_oalffp = FOREACH grpd_oalffps GENERATE group.PNR_ID AS PNR_ID, group.VERSION_NO AS VERSION_NO, group.PNR_PAX_ID AS PNR_PAX_ID, first_ffp::FFP_SUFFIX AS FFP_SUFFIX, first_ffp::FFP_NUMBER AS FFP_NUMBER;

-- calculated fields for PNR_HFX_GFXS - CTCR flag
filt_ctcr = FILTER cols_hfx_gfxs BY ( SERVICE_TYPE == 'OSI' AND VARIABLE_DATA MATCHES '.*CTCR.*' ); 
calc_ctcr = FOREACH filt_ctcr GENERATE PNR_ID, VERSION_NO, 'Y' AS CTCR_FLAG;
dist_ctcr = DISTINCT calc_ctcr;

-- calculated fields for PNR_HFX_GFXS - Skywards Number and Tier
filt_skwrd = FILTER cols_hfx_gfxs BY ( SERVICE_TYPE == 'OSI' AND VARIABLE_DATA MATCHES '1.*\\*EK.*' ); 
calc_skwrd = FOREACH filt_skwrd GENERATE PNR_ID, VERSION_NO, PNR_PAX_ID, 
	SUBSTRING( VARIABLE_DATA, INDEXOF( VARIABLE_DATA, '*EK' ) + 3, (int)SIZE( VARIABLE_DATA ) ) AS PNR_SKYWARDS_NUMBER,
	SUBSTRING( REPLACE( VARIABLE_DATA, '[\\s]', '' ), INDEXOF( REPLACE( VARIABLE_DATA, '[\\s]', '' ), '1' ) + 1, INDEXOF( REPLACE( VARIABLE_DATA, '[\\s]', '' ), '1', 1 ) ) AS PNR_TIER_CODE;
dist_skwrd = DISTINCT calc_skwrd;

-- calculated fields for PNR_OTHERS - emails
filt_emailso = FILTER cols_others BY ( OTHER_CODE == 'PHN' AND VARIABLE_DATA MATCHES 'E.*' ); 
calc_emailso1 = FOREACH filt_emailso GENERATE PNR_ID, VERSION_NO, 
	[ 'email_address', extract_pnremail(VARIABLE_DATA,'OTHERS'), 'source', 'PNR' ] as email;
calc_emailso2 = FILTER calc_emailso1 BY email#'email_address' IS NOT NULL;

grp_emailso = GROUP calc_emailso2 BY( PNR_ID, VERSION_NO );
grpd_emailso = FOREACH grp_emailso {
	email1 = FILTER calc_emailso2 BY email IS NOT NULL;
	email2 = email1.email;
	email3 = DISTINCT email2;
	GENERATE group.PNR_ID AS PNR_ID, group.VERSION_NO AS VERSION_NO, email3 AS PNR_EMAILS;
};

-- calculated fields for PNR_OTHERS - phones
filt_phoneso = FILTER cols_others BY ( OTHER_CODE == 'PHN' AND VARIABLE_DATA MATCHES '[M|H|B|P|T].*' ); 
calc_phoneso1 = FOREACH filt_phoneso GENERATE PNR_ID, VERSION_NO, 
	( VARIABLE_DATA MATCHES 'M.*' ? [ 'phone_number', REPLACE( REGEX_EXTRACT( REPLACE( TRIM( SUBSTRING( VARIABLE_DATA, INDEXOF( VARIABLE_DATA, 'M') + 1, (int)SIZE( VARIABLE_DATA ))), '-', '' ), '[\\d\\s-+]{5,}', 0 ), '[\\s-+]', '' ), 'phone_type', 'MOBILEPHONE', 'source', 'PNR' ] : 
	( VARIABLE_DATA MATCHES 'H.*' ? [ 'phone_number', REPLACE( REGEX_EXTRACT( REPLACE( TRIM( SUBSTRING( VARIABLE_DATA, INDEXOF( VARIABLE_DATA, 'H') + 1, (int)SIZE( VARIABLE_DATA ))), '-', '' ), '[\\d\\s-+]{5,}', 0 ), '[\\s-+]', '' ), 'phone_type', 'HOMEPHONE', 'source', 'PNR' ] : 
	( VARIABLE_DATA MATCHES 'B.*' ? [ 'phone_number', REPLACE( REGEX_EXTRACT( REPLACE( TRIM( SUBSTRING( VARIABLE_DATA, INDEXOF( VARIABLE_DATA, 'B') + 1, (int)SIZE( VARIABLE_DATA ))), '-', '' ), '[\\d\\s-+]{5,}', 0 ), '[\\s-+]', '' ), 'phone_type', 'BUSINESSPHONE', 'source', 'PNR' ] : 
	( VARIABLE_DATA MATCHES 'P.*' ? [ 'phone_number', REPLACE( REGEX_EXTRACT( REPLACE( TRIM( SUBSTRING( VARIABLE_DATA, INDEXOF( VARIABLE_DATA, 'P') + 1, (int)SIZE( VARIABLE_DATA ))), '-', '' ), '[\\d\\s-+]{5,}', 0 ), '[\\s-+]', '' ), 'phone_type', 'OTHERPHONE', 'source', 'PNR' ] : 
	( VARIABLE_DATA MATCHES 'T.*' ? [ 'phone_number', REPLACE( REGEX_EXTRACT( REPLACE( TRIM( SUBSTRING( VARIABLE_DATA, INDEXOF( VARIABLE_DATA, 'T') + 1, (int)SIZE( VARIABLE_DATA ))), '-', '' ), '[\\d\\s-+]{5,}', 0 ), '[\\s-+]', '' ), 'phone_type', 'AGENTPHONE', 'source', 'PNR' ] : 
		NULL ))))) AS phone;
calc_phoneso2 = FILTER calc_phoneso1 BY phone#'phone_number' IS NOT NULL;

grp_phoneso = GROUP calc_phoneso2 BY( PNR_ID, VERSION_NO );
grpd_phoneso = FOREACH grp_phoneso {
	phone1 = FILTER calc_phoneso2 BY phone IS NOT NULL;
	phone2 = phone1.phone;
	phone3 = DISTINCT phone2;
	GENERATE group.PNR_ID AS PNR_ID, group.VERSION_NO AS VERSION_NO, phone3 AS PNR_PHONES;
};

-- join the data together
join_step1 = JOIN calc_pnr BY( ID, VERSION_NO ), calc_pax BY( PNR_ID, VERSION_NO ), calc_itns BY( PNR_ID, VERSION_NO );
join_step2 = JOIN join_step1 BY( calc_pnr::inputfull_step2::inputfull_step1::ID, calc_pnr::inputfull_step2::inputfull_step1::VERSION_NO, calc_pax::PAX_ID ) LEFT OUTER, grpd_emails BY( PNR_ID, VERSION_NO, PNR_PAX_ID );
join_step2_1 = JOIN join_step2 BY( calc_pnr::inputfull_step2::inputfull_step1::ID, calc_pnr::inputfull_step2::inputfull_step1::VERSION_NO ) LEFT OUTER, grpd_emails_pnr_level BY( PNR_ID, VERSION_NO );
join_step3 = JOIN join_step2_1 BY( calc_pnr::inputfull_step2::inputfull_step1::ID, calc_pnr::inputfull_step2::inputfull_step1::VERSION_NO, calc_pax::PAX_ID ) LEFT OUTER, grpd_phones BY( PNR_ID, VERSION_NO, PNR_PAX_ID );
join_step3_1 = JOIN join_step3 BY( calc_pnr::inputfull_step2::inputfull_step1::ID, calc_pnr::inputfull_step2::inputfull_step1::VERSION_NO ) LEFT OUTER, grpd_phones_pnr_level BY( PNR_ID, VERSION_NO );
join_step4 = JOIN join_step3_1 BY( calc_pnr::inputfull_step2::inputfull_step1::ID, calc_pnr::inputfull_step2::inputfull_step1::VERSION_NO, calc_pax::PAX_ID ) LEFT OUTER, dist_oalffp BY( PNR_ID, VERSION_NO, PNR_PAX_ID );
join_step5 = JOIN join_step4 BY( calc_pnr::inputfull_step2::inputfull_step1::ID, calc_pnr::inputfull_step2::inputfull_step1::VERSION_NO ) LEFT OUTER, dist_ctcr BY( PNR_ID, VERSION_NO );
join_step6 = JOIN join_step5 BY( calc_pnr::inputfull_step2::inputfull_step1::ID, calc_pnr::inputfull_step2::inputfull_step1::VERSION_NO ) LEFT OUTER, grpd_emailso BY( PNR_ID, VERSION_NO );
join_step7 = JOIN join_step6 BY( calc_pnr::inputfull_step2::inputfull_step1::ID, calc_pnr::inputfull_step2::inputfull_step1::VERSION_NO ) LEFT OUTER, grpd_phoneso BY( PNR_ID, VERSION_NO );
join_step8 = JOIN join_step7 BY( calc_pnr::inputfull_step2::inputfull_step1::ID, calc_pnr::inputfull_step2::inputfull_step1::VERSION_NO, calc_pax::PAX_ID ) LEFT OUTER, dist_skwrd BY( PNR_ID, VERSION_NO, PNR_PAX_ID );

-- get the final output fields 
final = FOREACH join_step8 GENERATE 
	calc_pnr::inputfull_step2::inputfull_step1::REC_LOCATOR AS rec_locator,
	SUBSTRING(calc_pnr::inputfull_step2::inputfull_step1::PNR_CREATION_DATE,0,10) AS issue_date,
	calc_pax::PNR_PAX_NAME AS pax_name,
	calc_pnr::inputfull_step2::inputfull_step1::CREATOR_CITY AS creator_city,
	calc_pnr::inputfull_step2::inputfull_step1::POS_CRS_SYSTEM_DESIGNATOR AS pos_crs_system_designator,
	calc_pnr::inputfull_step2::inputfull_step1::POS_COUNTRY_CODE AS pos_country_code,
	calc_itns::BOARD_POINT AS board_point,
	calc_itns::OFF_POINT AS off_point,
	calc_itns::BOOKING_STATUS AS booking_status,
	SUBSTRING(calc_itns::DEPARTURE_DATE,0,10) AS departure_date,
	calc_itns::DEPARTURE_TIME AS departure_time,
	calc_itns::HOST_SEGMENT_NUMBER AS segment_number,
	calc_itns::OPERATING_AIRLINE AS operating_airline,
	calc_itns::MARKETING_AIRLINE AS marketing_airline,
	calc_itns::OPERATING_FLIGHT_NUMBER AS operating_flight_number,
	calc_itns::MARKETING_FLIGHT_NUMBER AS marketing_flight_number,
	calc_itns::CABIN_CLASS AS cabin_class,
	calc_itns::SERVICE_CLASS_CODE AS service_class,
	dist_oalffp::FFP_SUFFIX AS oal_ffp,
	dist_oalffp::FFP_NUMBER AS oal_ffp_pnr,
	( dist_ctcr::CTCR_FLAG IS NULL ? 'N' : dist_ctcr::CTCR_FLAG ) AS ctcr_flag,
	( grpd_emails::PNR_EMAILS IS NULL ? ( grpd_emailso::PNR_EMAILS IS NULL ? ( grpd_emails_pnr_level::PNR_EMAILS IS NULL ? NULL : grpd_emails_pnr_level::PNR_EMAILS ) : ( grpd_emails_pnr_level::PNR_EMAILS IS NULL ? grpd_emailso::PNR_EMAILS : BagConcat( grpd_emailso::PNR_EMAILS, grpd_emails_pnr_level::PNR_EMAILS ) ) ) : ( grpd_emailso::PNR_EMAILS IS NOT NULL AND grpd_emails_pnr_level::PNR_EMAILS IS NOT NULL ? BagConcat( grpd_emails::PNR_EMAILS, grpd_emailso::PNR_EMAILS, grpd_emails_pnr_level::PNR_EMAILS) : ( grpd_emailso::PNR_EMAILS IS NOT NULL ? BagConcat( grpd_emails::PNR_EMAILS, grpd_emailso::PNR_EMAILS) : ( grpd_emails_pnr_level::PNR_EMAILS IS NOT NULL ? BagConcat( grpd_emails::PNR_EMAILS, grpd_emails_pnr_level::PNR_EMAILS) : grpd_emails::PNR_EMAILS ) ) ) ) AS pnr_emails,
	( grpd_phones::PNR_PHONES IS NULL ? ( grpd_phoneso::PNR_PHONES IS NULL ? ( grpd_phones_pnr_level::PNR_PHONES IS NULL ? NULL : grpd_phones_pnr_level::PNR_PHONES ) : ( grpd_phones_pnr_level::PNR_PHONES IS NULL ? grpd_phoneso::PNR_PHONES : BagConcat( grpd_phoneso::PNR_PHONES, grpd_phones_pnr_level::PNR_PHONES ) ) ) : ( grpd_phoneso::PNR_PHONES IS NOT NULL AND grpd_phones_pnr_level::PNR_PHONES IS NOT NULL ? BagConcat( grpd_phones::PNR_PHONES, grpd_phoneso::PNR_PHONES, grpd_phones_pnr_level::PNR_PHONES) : ( grpd_phoneso::PNR_PHONES IS NOT NULL ? BagConcat( grpd_phones::PNR_PHONES, grpd_phoneso::PNR_PHONES) : ( grpd_phones_pnr_level::PNR_PHONES IS NOT NULL ? BagConcat( grpd_phones::PNR_PHONES, grpd_phones_pnr_level::PNR_PHONES) : grpd_phones::PNR_PHONES ) ) ) ) AS pnr_phones,
	dist_skwrd::PNR_SKYWARDS_NUMBER AS pnr_skywards_number,
	dist_skwrd::PNR_TIER_CODE AS pnr_tier_code,	
	calc_pnr::inputfull_step2::inputfull_step1::GROUP_BOOKING_IND AS grp_pnr_indicator;

-- store into target directory
SET mapred.output.compress false;
STORE final INTO '${TARGET_DIRECTORY}' USING JsonStorage();
