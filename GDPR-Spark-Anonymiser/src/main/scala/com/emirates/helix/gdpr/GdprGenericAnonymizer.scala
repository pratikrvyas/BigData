/*
*=====================================================================================================================================
* Created on     :   09/08/2018
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>GDPR
* Description    :   This generic spark 2 code for anonymization
* ======================================================================================================================================
*/

// ======================================================================================================================================
//  SPARK CMD 1 : INPUT TYPE : TABLE
// ---------------------------------
//  spark2-submit --master yarn --queue opseff --class "com.emirates.helix.GdprGenericAnonymizer" /home/ravindrac/GdprGenericAnonymizer-spark2.jar --EMCG_UUID_FILE /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/emcg_uuids.csv --COL_MASKING_FILE /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/column_masking_list.csv --MASKING_CONFIG_FILE /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/masking_prop.csv --INPUT_TABLE_NAME EMCG.H_SATELLITE_TICKET --INPUT_JOIN_KEY emcg_uuid --GDPR_REQUEST_TYPE delete --OUTPUT_PATH /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/Output/h_ticket_tbl --OUTPUT_FORMAT parquet --WRITE_MODE overwrite --COMPRESSION gzip --SPARK-PARTITIONS 5
// ---------------------------------
// SPARK CMD 2 : INPUT TYPE : FILE
// --------------------------------
// spark2-submit --master yarn --queue opseff --class "com.emirates.helix.GdprGenericAnonymizer" /home/ravindrac/GdprGenericAnonymizer-spark2.jar --EMCG_UUID_FILE /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/emcg_uuids.csv --COL_MASKING_FILE /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/column_masking_list.csv --MASKING_CONFIG_FILE /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/masking_prop.csv --INPUT_PATH /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/Output/h_ticket_tbl --INPUT_FORMAT parquet --INPUT_TABLE_NAME EMCG.H_SATELLITE_TICKET --INPUT_JOIN_KEY emcg_uuid --GDPR_REQUEST_TYPE delete --OUTPUT_PATH /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/Output/h_ticket_tbl --OUTPUT_FORMAT parquet --WRITE_MODE overwrite --COMPRESSION gzip --SPARK-PARTITIONS 5
// ======================================================================================================================================
// SPARK CMD 3 : GDPR REQUEST TYPE : delete
// ------------------------------------------
// spark2-submit --master yarn --queue opseff --class "com.emirates.helix.GdprGenericAnonymizer" /home/ravindrac/GdprGenericAnonymizer-spark2.jar --EMCG_UUID_FILE /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/emcg_uuids.csv --INPUT_TABLE_NAME EMCG.H_SATELLITE_TICKET --INPUT_JOIN_KEY emcg_uuid --GDPR_REQUEST_TYPE delete --OUTPUT_PATH /user/ravindrac/GDPR/CODE/Stroy_HEI11392_Anonymization/Output/h_ticket_tbl --OUTPUT_FORMAT parquet --WRITE_MODE overwrite --COMPRESSION gzip --SPARK-PARTITIONS 5


package com.emirates.helix.gdpr

import org.apache.log4j.{Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._


object GdprGenericAnonymizer extends  SparkEssentials {

  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("GdprGenericAnonymizer")

  def readColMaskFile(col_mask_file : String, spark : SparkSession) : (List[String],List[String],List[String],List[String],List[String],List[String]) ={
    import spark.implicits._
    val ip_mask_cols = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "false")
      .load(col_mask_file)

    val m_cols_list = ip_mask_cols.select(trim(lower($"column_name"))).map(r => r.getString(0)).collect.toList
    val m_col_type_list = ip_mask_cols.select(trim(lower($"column_type"))).map(r => r.getString(0)).collect.toList
    val m_col_key_list = ip_mask_cols.select(trim(lower($"key"))).map(r => r.getString(0)).collect.toList
    val m_start_ind_list = ip_mask_cols.select(trim($"start_index")).map(r => r.getString(0)).collect.toList
    val m_len_list = ip_mask_cols.select(trim($"length")).map(r => r.getString(0)).collect.toList
    val m_mask_val_list = ip_mask_cols.select(trim($"masking_value")).map(r => r.getString(0)).collect.toList

    (m_cols_list,m_col_type_list,m_col_key_list,m_start_ind_list,m_len_list,m_mask_val_list)
  }

  def readEmcgUuidFile(emcg_uuid_file : String, spark : SparkSession) : DataFrame = {

    val ip_emcg_uuids = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "false")
      .load(emcg_uuid_file)

    ip_emcg_uuids
  }

  def readMaskConfigFile(mask_config_file : String, spark : SparkSession) : (String,String,String,String,String) = {
    import spark.implicits._
    val ip_mask_prop = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "false")
      .load(mask_config_file)

    val DEFAULT_MASKING_VALUE = ip_mask_prop.where(trim($"property") === "DEFAULT_MASKING_VALUE").select(trim($"value")).map(r => r.getString(0)).collect.head
    val GDPR_RESTRICTED_FLAG_COL_NAME = ip_mask_prop.where(trim($"property") === "GDPR_RESTRICTED_FLAG_COL_NAME").select(trim($"value")).map(r => r.getString(0)).collect.head
    val GDPR_RESTRICTED_FLAG_COL_VALUE = ip_mask_prop.where(trim($"property") === "GDPR_RESTRICTED_FLAG_COL_VALUE").select(trim($"value")).map(r => r.getString(0)).collect.head
    val GDPR_REASON_CODE_COL_NAME = ip_mask_prop.where(trim($"property") === "GDPR_REASON_CODE_COL_NAME").select(trim($"value")).map(r => r.getString(0)).collect.head
    val GDPR_REASON_CODE_COL_VALUE = ip_mask_prop.where(trim($"property") === "GDPR_REASON_CODE_COL_ERASURE_VALUE").select(trim($"value")).map(r => r.getString(0)).collect.head

    (DEFAULT_MASKING_VALUE,GDPR_RESTRICTED_FLAG_COL_NAME,GDPR_RESTRICTED_FLAG_COL_VALUE,GDPR_REASON_CODE_COL_NAME,GDPR_REASON_CODE_COL_VALUE)
  }

  // FILTERING NON -ANONYMIZATION UUIDS FROM INPUT DATA
  def filterNonAnonymData(tbl_name : String, join_key : String,spark : SparkSession) : DataFrame = {

    val join_keys = join_key.split(",")
    var ip_data_na_uuids : DataFrame = null

    if (join_keys.size == 1) {

      // TIP : USE BROADCAST HASH JOIN
      ip_data_na_uuids = spark.sql(
        s"""
            select
            /*+ BROADCASTJOIN(a) */
            h.*
            from
          $tbl_name h
          left outer join
          A_UUIDS_TBL a
          on
          trim(h.$join_key) = trim(a.emcg_uuid)
          where a.emcg_uuid is null """
      )
    }
    else {
      var join_cond = ""
      join_keys.foreach { x =>
        join_cond = s"""trim(h.$x) = trim(a.emcg_uuid) or """ + join_cond
      }
      join_cond = join_cond.substring(0,join_cond.length - 4)
      // TIP : USE BROADCAST HASH JOIN
      ip_data_na_uuids = spark.sql(
        s"""
            select
            /*+ BROADCASTJOIN(a) */
            h.*
            from
          $tbl_name h
          left outer join
          A_UUIDS_TBL a
          on
          $join_cond
          where a.emcg_uuid is null """
      )
    }

    ip_data_na_uuids
  }

  // CHECK IF ANONYMIZATION COLUMNS PRESENT IN INPUT DATA
  def validateColMapping(ip_data_col_list : Array[String],m_cols_list : List[String]): Boolean ={

    val m_col_list_diff = m_cols_list.diff(ip_data_col_list)
    if(m_col_list_diff.nonEmpty){
      log.info("[ERROR] INVALID MASKING COLUMN NAMES : [" +  m_col_list_diff.mkString(",") + "]")
      log.info("[INFO] INPUT DATA COLUMN LIST : [" + ip_data_col_list.mkString(",") + "]")
      log.info("[INFO] PLEASE UPDATE THE COLUMN MASKING FILE AND TRY AGAIN")
      false
    }
    else{
      true
    }
  }

  def getColStartIndex(m_col_type : String ,m_start_ind : String,m_keys : String) : Any = {

    if(m_keys == null &&  (m_col_type.equalsIgnoreCase("map") || m_col_type.equalsIgnoreCase("struct"))){
      log.info("[INFO] IN MAP OR STRUCT COLUMNS KEY COLUMN IN MASKING FILE CANNOT BE EMPTY")
      System.exit(1)

    }

    if ( m_col_type.equalsIgnoreCase("map") || m_col_type.equalsIgnoreCase("struct") || m_col_type.equalsIgnoreCase("array[struct]") || m_col_type.equalsIgnoreCase("array[map]")){
      val m_keys_list = m_keys.split("#")
      val m_start_list = if(m_start_ind == null) m_keys_list.map{x => 99990000} else m_start_ind.split("#").map{x => toInt(x.trim) match {
        case Some(i) => i
        case None => 99990000
      }
      }

       m_start_list
    }
    else {
      val m_start_indx = toInt(m_start_ind) match {
        case Some(i) => i
        case None => 99990000
      }

        m_start_indx
    }
  }

  def getColLength(m_col_type : String ,m_col_len : String, m_keys : String) : Any = {

    if(m_keys == null && (m_col_type.equalsIgnoreCase("map") || m_col_type.equalsIgnoreCase("struct"))){
      log.info("[INFO] IN MAP OR STRUCT COLUMNS KEY COLUMN IN MASKING FILE CANNOT BE EMPTY")
      System.exit(1)
    }

    if ( m_col_type.equalsIgnoreCase("map") || m_col_type.equalsIgnoreCase("struct") || m_col_type.equalsIgnoreCase("array[struct]") || m_col_type.equalsIgnoreCase("array[map]")){
      val m_keys_list = m_keys.split("#")
      val m_len_list = if(m_col_len == null) m_keys_list.map{x => 99990000} else m_col_len.split("#").map{x => toInt(x.trim) match {
        case Some(i) => i
        case None => 99990000
      }
      }

       m_len_list
    }
    else {
      val m_len = toInt(m_col_len) match {
        case Some(i) => i
        case None => 99990000
      }

        m_len
    }
  }

  def complexTypesPreChecks(m_start_indx : Any, m_len : Any, m_keys : String, m_masks : String, col_name : String) : (Array[Int],Array[Int],Array[String],Array[String]) = {

    val m_key_start_list = m_start_indx.asInstanceOf[Array[Int]]
    val m_key_len_list = m_len.asInstanceOf[Array[Int]]
    val keys_list = m_keys.split("#")
    val masks = (toStr(m_masks) match {
      case Some(x) => x
      case None => ""
    })

    val mask_list = if(masks.isEmpty) keys_list.map{x => ""} else masks.split("#")
    // println(m_key_start_list.mkString(","))
    // println(m_key_len_list.mkString(","))

    if(m_key_len_list.size != m_key_start_list.size){
      log.info(s"[ERROR] MISS MATCH IN START INDEX AND LENGTH FOR THE COLUMN $col_name IN COLUMN MASKING FILE")
      // System.exit(1)
    }
    if(m_key_start_list.size != keys_list.size){
      log.info(s"[ERROR] MISS MATCH IN NO.OF KEYS AND (START_INDEX,LENGTH) FOR THE COLUMN $col_name IN COLUMN MASKING FILE")
      // System.exit(1)
    }
    if(m_key_len_list.size == 0 && m_key_start_list.size == 0){
      log.info(s"[ERROR] INVALID START_INDEX OR LENGTH FOR THE COLUMN $col_name IN COLUMN MASKING FILE")
      // System.exit(1)
    }

    (m_key_start_list,m_key_len_list,keys_list,mask_list)
  }


  def mapCreator(keys_list : Array[String],m_key_start_list :  Array[Int], m_key_len_list : Array[Int], mask_list : Array[String], default_mask : String, col : String) : String = {

    // val y = "map('name',concat('xx',mapping['name']),'dep','xxxx')"
    var map_qry = "map('"

    keys_list.foreach{key =>
      map_qry = map_qry + key + "',"
      val x_ind = keys_list.indexOf(key)
      val start =  m_key_start_list(x_ind)
      val len = m_key_len_list(x_ind)
      val mask_val = toStr(if(mask_list(x_ind).isEmpty) null.asInstanceOf[String] else mask_list(x_ind)) match {
      case Some(x) => x
      case None => default_mask
      }

      if (start == 99990000 && len != 99990000) {
        log.info("[ERROR] INVALID MASKING COLUMN START INDEX OR LENGTH FOR THE COLUMN : " + col)
        System.exit(1)
      }
      else if (start == 99990000 && len == 99990000) {

        map_qry = map_qry + "IF(isnull(IF(lower(" + "h." + col + "['"  + key + "'])='null',null," + "h." + col + "['" + key + "']))," + "h." + col + "['" + key +"'],'" + mask_val + "'), '"

      }
      else if (start != 99990000 && len != 99990000) {

        map_qry = map_qry + "concat(substring(cast(" + "h." + col + "['" + key + "'] as String)," + 0 +
          "," + start + "), '" + mask_val + "', substring(cast(" + "h." + col + "['" + key + "'] as String)," + (start.toString.toInt + len.toString.toInt + 1) +
          ", length(" + "h." + col + "['" + key + "']))), '"

      }
    }

    map_qry = (map_qry + "$$map").replace(", '$$map",")")

     map_qry
  }

  def setUpdateMapUDF(spark : SparkSession) = {
    val updateMap = spark.udf.register("updateMap", (x: Map[String, String], y: Map[String, String]) => {
      if(y != null && x != null) x ++ y else null
    })
  }

  def structCreator(keys_list : Array[String],m_key_start_list :  Array[Int], m_key_len_list : Array[Int], mask_list : Array[String], default_mask : String, col : String,
                    input_df : DataFrame) : String = {
    // struct("+filter_nested_fields_str+",'xxxxx' as is_mammal) as animal_interpretation
    var struct_qry = "struct("

    // FILTERING NON ANONYMIZED STRUCT FIELDS
    val filter_nested_fields = input_df.schema.filter(c => c.name == col).flatMap(_.dataType.asInstanceOf[StructType].fields).
      map(_.name).filterNot(x => keys_list.map(y => y.toLowerCase).contains(x.toLowerCase)).map(x=>col + "." + x).mkString(",")

    struct_qry = struct_qry + filter_nested_fields + ","
    keys_list.foreach{key =>
      val x_ind = keys_list.indexOf(key)
      val start =  m_key_start_list(x_ind)
      val len = m_key_len_list(x_ind)
      val mask_val = toStr(if(mask_list(x_ind).isEmpty) null.asInstanceOf[String] else mask_list(x_ind)) match {
        case Some(x) => x
        case None => default_mask
      }

      if (start == 99990000 && len != 99990000) {
        log.info("[ERROR] INVALID MASKING COLUMN START INDEX OR LENGTH FOR THE COLUMN : " + col)
        System.exit(1)
      }
      else if (start == 99990000 && len == 99990000) {

        struct_qry = struct_qry + "IF(isnull(IF(lower(" + "h." + col + "['"  + key + "'])='null',null," + "h." + col + "['" + key + "']))," + "h." + col + "['" + key +"'],'" + mask_val + "') as " + key +", "

      }
      else if (start != 99990000 && len != 99990000) {

        struct_qry = struct_qry + "concat(substring(cast(" + "h." + col + "['" + key + "'] as String)," + 0 +
          "," + start + "), '" + mask_val + "', substring(cast(" + "h." + col + "['" + key + "'] as String)," + (start.toString.toInt + len.toString.toInt + 1) +
          ", length(" + "h." + col + "['" + key + "']))) as " + key + ", "

      }
    }

    struct_qry = (struct_qry + "$$struct").replace(", $$struct",")")

    struct_qry
  }

  def removeStringArrayDuplicates(spark : SparkSession) ={
    import scala.collection.mutable.WrappedArray
    val removeDuplicates: WrappedArray[String] => WrappedArray[String] = _.distinct
    val removeStringArrayDuplicatesUDF = spark.udf.register("removeStringArrayDuplicatesUDF",removeDuplicates)

  }

  def buildAnonymizedQry(ip_data_col_list : Array[String],m_cols_list : List[String],m_col_type_list : List[String],m_col_key_list : List[String],m_start_ind_list : List[String]
                        ,m_len_list : List[String],m_mask_val_list : List[String],DEFAULT_MASKING_VALUE : String
                        ,GDPR_RESTRICTED_FLAG_COL_VALUE : String,GDPR_RESTRICTED_FLAG_COL_NAME : String,GDPR_REASON_CODE_COL_VALUE : String
                         ,GDPR_REASON_CODE_COL_NAME : String,input_tbl_name : String,ip_join_key : String,m_join_key : String,input_df : DataFrame) : (String,Array[String],Array[String]) = {


    var anon_sql_qry = "select /*+ BROADCASTJOIN(a) */ "
    var array_of_map = new scala.collection.mutable.ArrayBuffer[String]
    var array_of_struct = new scala.collection.mutable.ArrayBuffer[String]


    ip_data_col_list.foreach{col =>
      if(m_cols_list.contains(col)) {

        // GETTING INDEX AND PARSING THE COLUMN MAPPING FILE
        val m_col_indx = m_cols_list.indexOf(col)
        val m_col_type = toStr(m_col_type_list(m_col_indx)) match {
          case Some(x) => x
          case None => ""
        }
        val m_start_indx = getColStartIndex(m_col_type,m_start_ind_list(m_col_indx),m_col_key_list(m_col_indx))
        val m_len = getColLength(m_col_type,m_len_list(m_col_indx),m_col_key_list(m_col_indx))
        val mask_val = toStr(m_mask_val_list(m_col_indx)) match {
          case Some(x) => x
          case None => DEFAULT_MASKING_VALUE
        }

        // ADDING SUPPORT FOR MAP TYPE
        if (m_col_type.equalsIgnoreCase("map")) {

          // PRE CHECKS TO THE COMPLEX COLUMNS
          val (m_key_start_list,m_key_len_list,keys_list,mask_list) = complexTypesPreChecks(m_start_indx,m_len,m_col_key_list(m_col_indx),mask_val,col)

          // CHECKING IF THE MAP COLUMN IS ALREADY PROCESSED
          var col_chk_list =  new scala.collection.mutable.ListBuffer[String]()
          if (!col_chk_list.contains(col)) {
            col_chk_list += col

            // CREATE SQL MAP
            val map_qry = mapCreator(keys_list,m_key_start_list, m_key_len_list,mask_list,DEFAULT_MASKING_VALUE,col)
            log.info(s"[INFO] DYNAMIC MAP QUERY FOR THE $col CREATED SUCCESSFULLY : $map_qry")

            // sql(s"select updateMap(mapping,$y) from trg_tbl").show(false)
            anon_sql_qry = anon_sql_qry + "updateMap(" + col + ","  + map_qry +") as " + col + ", "
          }
          else{
            log.info(s"[ERROR] COLUMN NAME ($col) IS REPEATED IN COLUMN MASKING FILE. PLEASE ENSURE COLUMN NAMES ARE NOT DUPLICATED")
            System.exit(1)
          }

        }
        // ADDING SUPPORT FOR STRUCT TYPE
        else if (m_col_type.equalsIgnoreCase("struct")) {

          // PRE CHECKS TO THE COMPLEX COLUMNS
          val (m_key_start_list,m_key_len_list,keys_list,mask_list) = complexTypesPreChecks(m_start_indx,m_len,m_col_key_list(m_col_indx),m_mask_val_list(m_col_indx),col)

          // CHECKING IF THE STRUCT COLUMN IS ALREADY PROCESSED
          var col_chk_list =  new scala.collection.mutable.ListBuffer[String]()
          if (!col_chk_list.contains(col)) {
            col_chk_list += col

            // CREATE SQL STRUCT
            val struct_qry = structCreator(keys_list,m_key_start_list, m_key_len_list,mask_list,DEFAULT_MASKING_VALUE,col,input_df)

            log.info(s"[INFO] DYNAMIC STRUCT QUERY FOR THE $col CREATED SUCCESSFULLY : $struct_qry")

            anon_sql_qry = anon_sql_qry + struct_qry + " as " + col + ", "
          }
          else{
            log.info(s"[ERROR] COLUMN NAME ($col) IS REPEATED IN COLUMN MASKING FILE. PLEASE ENSURE COLUMN NAMES ARE NOT DUPLICATED")
            System.exit(1)
          }

        }
        else if (m_col_type.equalsIgnoreCase("array[map]")) {

          array_of_map += col
          anon_sql_qry = anon_sql_qry + "h." + col + ", "

        }
        else if (m_col_type.equalsIgnoreCase("array[struct]")) {

          array_of_struct += col
          anon_sql_qry = anon_sql_qry + "h." + col + ", "

        }
        // SIMPLE COLUMNS
        else {
          // PRECHECKS TO SIMPLE COLUMNS
          if (m_start_indx == 99990000 && m_len != 99990000) {
            log.info("[ERROR] INVALID MASKING COLUMN START INDEX OR LENGTH FOR THE COLUMN : " + col)
            System.exit(1)
          }
          else if (m_start_indx == 99990000 && m_len == 99990000) {
            // sql("select IF(isnull(IF(lower('null')='null',null,'xxxx')),null,'xxxxx')")
            anon_sql_qry = anon_sql_qry + "IF(isnull(IF(lower(" + col + ")='null',null," + col + "))," + col + ",'" + mask_val + "') as " + col + ", "
            // anon_sql_qry = anon_sql_qry +"'"+ mask_val + "' as " + col + ", "
          }
          else if (m_start_indx != 99990000 && m_len != 99990000) {
            anon_sql_qry = anon_sql_qry + "concat(substring(cast(" + "h." + col + " as String)," + 0 +
              "," + m_start_indx + "), '" + mask_val + "', substring(cast(" + "h." + col + " as String)," + (m_start_indx.toString.toInt + m_len.toString.toInt + 1) +
              ", length(" + "h." + col + "))) AS " + col + ", "
          }
        }
      }
        else{
          if(GDPR_RESTRICTED_FLAG_COL_VALUE.equalsIgnoreCase("Y") && GDPR_RESTRICTED_FLAG_COL_NAME.equalsIgnoreCase(col)){
            anon_sql_qry = anon_sql_qry + "'Y' as " + col + ", "
          }
          else if(GDPR_REASON_CODE_COL_VALUE.nonEmpty && GDPR_REASON_CODE_COL_NAME.equalsIgnoreCase(col)){
            if(getColType(input_df,col).equalsIgnoreCase("array")){
              println("working - gdpr")
              //  sql("select removeDuplicates(split(concat_ws(',',hit_songs,'hey jude'),',')) as cc from tbl").show(false)
              val gdpr_col_qry = "removeStringArrayDuplicatesUDF(split(concat_ws(','," + col + ",'" + GDPR_REASON_CODE_COL_VALUE + "'),','))" + " as " + col + ", "
              anon_sql_qry = anon_sql_qry + gdpr_col_qry

            }
            else {
              anon_sql_qry = anon_sql_qry + "'" + GDPR_REASON_CODE_COL_VALUE + "' as " + col + ", "
            }
          }
          else{
            anon_sql_qry = anon_sql_qry + "h." + col + ", "
          }
        }
      }

      val join_keys = ip_join_key.split(",")

      var join_cond = ""
      join_keys.foreach { x =>
        join_cond = s"""trim(h.$x) = trim(a.emcg_uuid) or """ + join_cond
      }
      join_cond = join_cond.substring(0,join_cond.length - 4)

      val m_join_qry = if (join_keys.size == 1) input_tbl_name + " h inner join A_UUIDS_TBL a on trim(h." + ip_join_key + ") = trim(a." + m_join_key + ")"
      else input_tbl_name + " h inner join A_UUIDS_TBL a on " + join_cond

      anon_sql_qry = (anon_sql_qry + "$$from " + m_join_qry).replace(", $$from"," from")

      log.info("[ANONY QUERY]" + anon_sql_qry)

    (anon_sql_qry,array_of_map.toArray,array_of_struct.toArray)
  }

  def explodeOuter(df: DataFrame, columnsToExplod: String) : DataFrame = {
    val arrayFields = df.schema.fields.map(field => field.name -> field.dataType)
      .collect{case (name: String, type1 : ArrayType) => (name, type1.asInstanceOf[ArrayType])}
      .toMap

    val resutl_df = df.withColumn(columnsToExplod, explode(when(col(columnsToExplod).isNotNull, col(columnsToExplod))
          .otherwise(array(lit(null).cast(arrayFields(columnsToExplod).elementType)))))

     resutl_df
  }


    // FOR ARRAY[MAPS] & ARRAY[STRUCTS]
    def recursiveMapComplexTypes(df : DataFrame,prev_df : DataFrame,join_keys : String,explode_keys : String,cmplx_columns : Array[String],m_cols_list : List[String],
                                 m_col_key_list : List[String], m_col_type_list : List[String], m_start_ind_list : List[String], m_len_list : List[String], m_mask_val_list : List[String],
                                 DEFAULT_MASKING_VALUE : String, prev_col : String , spark : SparkSession) : DataFrame = {

      if(cmplx_columns.isEmpty) {
         prev_df
      }
      else{
        val cmplx_col = cmplx_columns.head

        // EXPLODE THE ARRAY OF MAP TO ANONYMIZE
        df.createOrReplaceTempView("anon_tbl")

        // GIVES NON EXPLODED COLUMNS EXCLUDING ANONYMIZED COMPLEX COLS
        // val df_fil_cols = cmplx_columns.filterNot(x => x.equalsIgnoreCase(cmplx_col)).mkString(",")
       //  val df_filter_cols = if(prev_col.isEmpty) df_fil_cols else df_fil_cols + "," + prev_col

        val expld_qry = "select ".concat(if(explode_keys.equals("")) join_keys else join_keys + ", " + explode_keys)

        // val df_fil_qry = if(df_fil_cols.isEmpty) expld_qry else expld_qry + ", "+ df_filter_cols
        val cmplx_df = spark.sql("" + expld_qry  + " ," + cmplx_col + " from anon_tbl h")

        val expld_df = explodeOuter(cmplx_df,cmplx_col)

        // println("exploded df")
        // expld_df.show(false)

        val m_col_indx = m_cols_list.indexOf(cmplx_col)
        val m_col_type = toStr(m_col_type_list(m_col_indx)) match {
          case Some(x) => x
          case None => ""
        }
        val m_start_indx = getColStartIndex(m_col_type,m_start_ind_list(m_col_indx),m_col_key_list(m_col_indx))
        val m_len = getColLength(m_col_type,m_len_list(m_col_indx),m_col_key_list(m_col_indx))
        val mask_val = toStr(m_mask_val_list(m_col_indx)) match {
          case Some(x) => x
          case None => DEFAULT_MASKING_VALUE
        }

        // PRE CHECKS TO THE COMPLEX COLUMNS
        val (m_key_start_list,m_key_len_list,keys_list,mask_list) = complexTypesPreChecks(m_start_indx,m_len,m_col_key_list(m_col_indx),m_mask_val_list(m_col_indx),cmplx_col)

        // CREATE SQL MAP
        val map_qry = mapCreator(keys_list,m_key_start_list, m_key_len_list,mask_list,DEFAULT_MASKING_VALUE,cmplx_col)
        log.info(s"[INFO] DYNAMIC MAP QUERY FOR THE ARRAY[MAP] COLUMN $cmplx_col CREATED SUCCESSFULLY : $map_qry")

        // RE-CONSTRUCT THE UPDATE MAP WITH ANONYMIZATION
        expld_df.createOrReplaceTempView("expld_tbl")
        val fil_cols = expld_df.columns.filterNot(x => x.equalsIgnoreCase(cmplx_col)).mkString(",")
        val filter_cols = if(prev_col.isEmpty) fil_cols else fil_cols // + "," + prev_col
        val m_sql_qry = "select " + filter_cols +", updateMap(" + cmplx_col + ","  + map_qry +") as " + cmplx_col + " from expld_tbl h"
        log.info(s"[INFO] DYNAMIC SQL QUERY FOR THE ARRAY[MAP] COLUMN $cmplx_col CREATED SUCCESSFULLY : $m_sql_qry")
        val map_df =  spark.sql(m_sql_qry)

        // RE-CONSTRUCT THE ARRAY OF MAP
        map_df.createOrReplaceTempView("a_map_tbl")
        val a_sql_qry = "select " + filter_cols + ", if(size(collect_list(" + cmplx_col + ")) <= 0,null,collect_list(" + cmplx_col + ")) as " + cmplx_col + " from a_map_tbl h group by " + filter_cols
        log.info(s"[INFO] DYNAMIC ARRAY SQL QUERY FOR THE ARRAY[MAP] COLUMN $cmplx_col CREATED SUCCESSFULLY : $a_sql_qry")
        val array_map_df =  spark.sql(a_sql_qry)

        val current_df = if(prev_df == null) array_map_df else explodeJoin(prev_df,array_map_df,join_keys,explode_keys)

        recursiveMapComplexTypes(df,current_df,join_keys,explode_keys,cmplx_columns.tail,m_cols_list,
          m_col_key_list, m_col_type_list, m_start_ind_list, m_len_list, m_mask_val_list,
          DEFAULT_MASKING_VALUE, cmplx_col, spark)

      }
    }

  // TO VALIDATE COUNTS OF INPUT DATA AND OUTPUT DATA
  def validateCounts(op_df : Long,input_tbl_name : String,spark : SparkSession ) : Boolean = {

    val ip_data_cnt = spark.sql(s"""select * from $input_tbl_name""").count
    val an_data_cnt = op_df

    if( an_data_cnt != ip_data_cnt ){
      log.info("[ERROR] ANONYMIZATION IS COMPLETED PARTIALLY WITH ERRORS [INPUT DATA COUNT : " + ip_data_cnt + ", OUTPUT DATA COUNT : " + an_data_cnt + "]")
      false
    }else
    {
      log.info("[INFO] ANONYMIZATION IS COMPLETED SUCCESSFULLY [INPUT DATA COUNT : " + ip_data_cnt + ", OUTPUT DATA COUNT : " + an_data_cnt + "]")
      true
    }
  }

  def explodeJoin(df1 : DataFrame,df2 : DataFrame, join_keys : String, explode_keys : String) : DataFrame = {

    val keys = (if(explode_keys.equals("")) join_keys else join_keys + "," + explode_keys).split(",")
    val expld_cols = df2.columns.filterNot(x => keys.contains(x))
    val filter_cols = df1.columns.filterNot(x => expld_cols.contains(x))


    val joinExprs = keys
      .zip(keys)
      .map{case (c1, c2) => df1(c1) <=> df2(c2)}
      .reduce(_ && _)

    log.info("[JOIN EXPR]" + joinExprs)

    val colNames = filter_cols.map(name => col(name))
    val keyNames = keys.toSeq

    val result_df = df1.select(colNames:_*).join(df2,keyNames,"inner")
    result_df
  }

  def anonymizeFullColumns(df : DataFrame, columns : Array[String],default_mask : String) : DataFrame = {

    val result_df = columns.foldLeft(df) { (memoDF, colName) =>
        memoDF.withColumn(colName,lit(default_mask))
      }

    result_df
  }

  def joinKeyNullRec(anony_df : DataFrame,join_keys : String, explode_keys : String, spark : SparkSession) : DataFrame = {

    val keys = (if(explode_keys.equals("")) join_keys else join_keys + "," + explode_keys).split(",")
    val where_qry = ("where " + keys.foldLeft("")((x,y) => x + " " + y + " is null or") + "$$").replace(" or$$","")

    anony_df.createOrReplaceTempView("anon_reject_tbl")

    val anon_rejected = spark.sql("select * from anon_reject_tbl " +where_qry)

    anon_rejected
  }

 }
