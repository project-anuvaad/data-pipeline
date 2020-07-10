# -*- coding: utf-8 -*-

######################################################
# PROJECT : Anuvaad Data Platform Contract
# AUTHOR  : Tarento Technologies
# DATE    : Jul 02, 2020
######################################################


import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

SCHEMA_INPUT_FILE = StructType([ \
                          StructField("sno", StringType(), True), \
                          StructField("src", StringType(), True), \
                          StructField("tgt", StringType(), True), \
                          StructField("src_lang", StringType(), True), \
                          StructField("tgt_lang", StringType(), True), \
                          StructField("metadata_json", StringType(), True)])

df     = spark.read.format("com.databricks.spark.csv") \
                      .option("header", "true") \
                      .option("multiline","true") \
                      .option("escape", "\"") \
                      .schema(SCHEMA_INPUT_FILE) \
                      .load("/user/root/bd-platform/input/rs_aligned_json_metadata.csv")

metadata_schema =  StructType([ StructField("src_filename", StringType(), True),
                           StructField("tgt_filename", StringType(), True),
                          StructField("label", StringType(), True)
                        ])

df = df.withColumn("metadata_json_parsed", F.from_json(df["metadata_json"], metadata_schema))

df = df.select(F.sha2(F.col("src"), 256).alias("src_hash"), "src", "tgt", "src_lang", "tgt_lang", \
              "metadata_json_parsed.src_filename", \
              "metadata_json_parsed.tgt_filename", \
              "metadata_json_parsed.label")

df.write.parquet("/user/root/hive/proto/proto.parquet")


######################################################
# Exposing in spark-sql
######################################################
#  > create database test_db;
#  > use test_db;
#  > create table sentences_parquet(
#      src_hash string,
#      src string,
#      tgt string,
#      src_lang string,
#      tgt_lang string,
#      src_filename string,
#      tgt_filename string,
#      label string)
#    stored as parquet 
#    location '/user/root/hive/proto/proto.parquet';
#  
#  > desc sentences_parquet;
#  +---------------+------------+----------+
#  |   col_name    | data_type  | comment  |
#  +---------------+------------+----------+
#  | src_hash      | string     |          |
#  | src           | string     |          |
#  | tgt           | string     |          |
#  | src_lang      | string     |          |
#  | tgt_lang      | string     |          |
#  | src_filename  | string     |          |
#  | tgt_filename  | string     |          |
#  | label         | string     |          |
#  +---------------+------------+----------+
#  
#  > select * from sentences_parquet where sno in ('5', '7');
#  
#  
######################################################
# Using PySpark
######################################################
#  >>> spark.sql("""
#      create table sentences_parquet(
#        sno string,
#        src string,
#        tgt string,
#        src_lang string,
#        tgt_lang string,
#        src_filename string,
#        tgt_filename string,
#        label string)
#       stored as parquet
#       location '/tmp/proto.parquet'
#      """)
#  
#  spark.sql("show tables").show()
#  spark.sql("select src, tgt from sentences_parquet2 where label='M' limit 10").show()
#  
#  
#   >>> df = spark.sql("select src, tgt, label from sentences_parquet where length(src) < 40")
#   >>> df.filter("label='M'").select("src", "tgt").show(10, False)
#   +--------------------------------------+-------------------------------------+
#   |src                                   |tgt                              	|
#   +--------------------------------------+-------------------------------------+
#   |B. B.                                 |बी.                             		|
#   |Tiwari passed away on 25 April, 2012. |तिवारी का 25 अप्रैल, 2012 को निधन हो गया। 	|
#   |Working of 3 Ministries was discussed.|तीन मंत्रालयों के कार्यकरण पर चर्चा हुई।      	|
#   |Some time was lost in disruptions;    |थोड़ा समय व्यवधान में नष्ट हुआ;            	|
#   |Even they were not discussed.         |इन पर भी चर्चा नहीं हुई।                  	|
#   |Private Members introduced 33 Bills.  |गैर-सरकारी सदस्यों ने 33 विधेयक पुरःस्थापित किए 	|
#   |This has been a difficult year.       |यह एक कठिन वर्ष रहा है।              		|
#   |In all, the House sat for 22 sittings.|कुल मिलाकर सभा की 22 बैठकें हुईं हैं।       	|
#   |This has been a difficult year.       |यह एक कठिन वर्ष रहा है।            		|
#   |That must really inspire us.          |हमें वास्तव में उससे प्रेरित होना चाहिए।     		|
#   +--------------------------------------+-------------------------------------+
#   
#   
#   spark-sql> select src, tgt, label from sentences_parquet where length(src) < 40 and label='M' limit 10;
#   
#   B. B.	बी.	M
#   Tiwari passed away on 25 April, 2012.	तिवारी का 25 अप्रैल, 2012 को निधन हो गया।	M
#   Working of 3 Ministries was discussed.	तीन मंत्रालयों के कार्यकरण पर चर्चा हुई।	M
#   Some time was lost in disruptions;	थोड़ा समय व्यवधान में नष्ट हुआ;	M
#   Even they were not discussed.	इन पर भी चर्चा नहीं हुई।	M
#   Private Members introduced 33 Bills.	गैर-सरकारी सदस्यों ने 33 विधेयक पुरःस्थापित किए।	M
#   This has been a difficult year.	यह एक कठिन वर्ष रहा है।	M
#   In all, the House sat for 22 sittings.	कुल मिलाकर सभा की 22 बैठकें हुईं हैं।	M
#   This has been a difficult year.	यह एक कठिन वर्ष रहा है।	M
#   That must really inspire us.	हमें वास्तव में उससे प्रेरित होना चाहिए।	M
#
######################################################
# Loading into HBase
######################################################
#  CREATE EXTERNAL TABLE hbase_table_sentences
#    (
#       src_hash string,
#       src string,
#       tgt string,
#       src_lang string,
#       tgt_lang string,
#       src_filename string,
#       tgt_filename string,
#       label string
#    )
#  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
#  WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:src,cf:tgt,cf:src_lang,cf:tgt_lang,cf:src_filename,cf:tgt_filename,cf:label")
#  TBLPROPERTIES ("hbase.table.name" = "hbase_sentences");
#  
#  
#  INSERT INTO TABLE hbase_table_sentences SELECT * FROM sentences_parquet;