# -*- coding: utf-8 -*-

######################################################
# PROJECT : All pair sentence salvage
# AUTHOR  : Tarento Technologies
# DATE    : May 05, 2020
######################################################


from pyspark.sql import SparkSession


from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
import pyspark.sql.functions as F

'''
Given set of salvaged English sentences, find all the translated version
in other languages

'''


'''
---------------------------------------
SPARK SESSION CREATION
---------------------------------------
'''
spark = SparkSession \
    .builder \
    .appName("All_Pairs_Sentences") \
    .enableHiveSupport() \
    .getOrCreate()


spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.executor.cores", 6)
spark.conf.set("spark.dynamicAllocation.minExecutors","3")
spark.conf.set("spark.dynamicAllocation.maxExecutors","6")


LOG4JLOGGER = spark.sparkContext._jvm.org.apache.log4j
LOGGER = LOG4JLOGGER.LogManager.getLogger(__name__)
LOGGER.info("-------------------------------------------------------")
LOGGER.info("Starting all-pair salvaged pair Sentence ...")
LOGGER.info("-------------------------------------------------------")



# Read clean sentences (215k records)
SCHEMA_CLEAN_INPUT_FILE = StructType([
                             StructField("original_sent", StringType(), True),
                             StructField("sentence_dup", StringType(), True),
                             StructField("hash_val", StringType(), True),
                             StructField("pdf_filename", StringType(), True),
                             ])

# CLEAN_INPUT_FILE="/user/root/exact-match-score/output-revised/*"
CLEAN_INPUT_FILE="/Users/TIMAC044/Documents/Anuvaad/parallel-sentences.csv"
CLEAN_INPUT_FILE_DF = spark.read.format("com.databricks.spark.csv") \
                  .option("delimiter", ',') \
                  .option("quote", "\"") \
                  .option("escape", "\"") \
                  .option("header", "false") \
                  .schema(SCHEMA_CLEAN_INPUT_FILE) \
                  .load(CLEAN_INPUT_FILE)

CLEAN_INPUT_FILE_DF = CLEAN_INPUT_FILE_DF.select("original_sent") \
                   .withColumn("sentence", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))

CLEAN_INPUT_FILE_DF.createOrReplaceTempView('CLEAN_INPUT_FILE_DF')



# #########################################################
# 1. Load TAMIL 
# #########################################################
#EN_INPUT_FILE="/user/root/parallel-sentences/ta/source_en_ik_06_02_20_2.txt"
EN_INPUT_FILE="/Users/TIMAC044/Documents/parallel-sentences/ta/source_en_ik_06_02_20_2.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

#TGT_INPUT_FILE="/user/root/parallel-sentences/ta/target_ta_ik_06_02_20_2.txt"
TGT_INPUT_FILE="/Users/TIMAC044/Documents/parallel-sentences/ta/target_ta_ik_06_02_20_2.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

TA_FINAL = spark.sql(' \
                        SELECT DISTINCT b.original_sent as src_sentence, c.value as tgt_sentence \
                        FROM EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE b.row_id = c.row_id')

TA_FINAL.createOrReplaceTempView('TA_FINAL')

FINAL = spark.sql(' \
                        SELECT \
                            a.original_sent as pdf_sentence, \
                            tgt.tgt_sentence, \
                            hash(a.sentence) \
                        FROM \
                            CLEAN_INPUT_FILE_DF a \
                        LEFT OUTER JOIN \
                            TA_FINAL tgt \
                        ON \
                            hash(a.original_sent) = hash(tgt.src_sentence) \
                ')

FINAL.write.csv('/user/root/ta-overlap-in-215k-sent')

# #########################################################
# 2. Load HINDI 
# #########################################################
EN_INPUT_FILE="/user/root/parallel-sentences/hi/source_en_ik_06_02_20_02.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

TGT_INPUT_FILE="/user/root/parallel-sentences/hi/target_hi_ik_06_02_20_02.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

HI_FINAL = spark.sql(' \
                        SELECT DISTINCT b.value as src_sentence, c.value as tgt_sentence, b.row_id\
                        FROM CLEAN_INPUT_FILE_DF a, EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE hash(a.sentence) = hash(b.value) AND b.row_id = c.row_id')


# #########################################################
# 3. Load BENGALI 
# #########################################################
EN_INPUT_FILE="/user/root/parallel-sentences/bn/source_en_ik_06_02_20_02.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

TGT_INPUT_FILE="/user/root/parallel-sentences/bn/target_bn_ik_06_02_20_02.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

BN_FINAL = spark.sql(' \
                        SELECT DISTINCT b.value as src_sentence, c.value as tgt_sentence \
                        FROM CLEAN_INPUT_FILE_DF a, EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE hash(a.sentence) = hash(b.value) AND b.row_id = c.row_id')



# #########################################################
# 4. Load GUJARATI 
# #########################################################
EN_INPUT_FILE="/user/root/parallel-sentences/gu/source_en_ik_06_02_20_02.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

TGT_INPUT_FILE="/user/root/parallel-sentences/gu/target_gu_ik_06_02_20_02.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

GU_FINAL = spark.sql(' \
                        SELECT DISTINCT b.value as src_sentence, c.value as tgt_sentence \
                        FROM CLEAN_INPUT_FILE_DF a, EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE hash(a.sentence) = hash(b.value) AND b.row_id = c.row_id')



# #########################################################
# 5. Load KANNAD 
# #########################################################
EN_INPUT_FILE="/user/root/parallel-sentences/ka/source_en_ik_10_02_20_02.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

TGT_INPUT_FILE="/user/root/parallel-sentences/ka/target_kn_ik_10_02_20_02.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

KA_FINAL = spark.sql(' \
                        SELECT DISTINCT b.value as src_sentence, c.value as tgt_sentence \
                        FROM CLEAN_INPUT_FILE_DF a, EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE hash(a.sentence) = hash(b.value) AND b.row_id = c.row_id')



# #########################################################
# 6. Load MALAYALAM 
# #########################################################
EN_INPUT_FILE="/user/root/parallel-sentences/ml/source_en_ik_06_02_20_02.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

TGT_INPUT_FILE="/user/root/parallel-sentences/ml/target_ml_ik_06_02_20_02.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

ML_FINAL = spark.sql(' \
                        SELECT DISTINCT b.value as src_sentence, c.value as tgt_sentence \
                        FROM CLEAN_INPUT_FILE_DF a, EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE hash(a.sentence) = hash(b.value) AND b.row_id = c.row_id')



# #########################################################
# 7. Load MARATHI 
# #########################################################
EN_INPUT_FILE="/user/root/parallel-sentences/mr/source_en_ik_27_01_20_02.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

TGT_INPUT_FILE="/user/root/parallel-sentences/mr/target_mr_ik_27_01_20_02.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

MR_FINAL = spark.sql(' \
                        SELECT DISTINCT b.value as src_sentence, c.value as tgt_sentence \
                        FROM CLEAN_INPUT_FILE_DF a, EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE hash(a.sentence) = hash(b.value) AND b.row_id = c.row_id')



# #########################################################
# 8. Load PUNJABI 
# #########################################################
EN_INPUT_FILE="/user/root/parallel-sentences/pa/source_en_ik_06_02_20_02.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

TGT_INPUT_FILE="/user/root/parallel-sentences/pa/target_pa_ik_06_02_20_02.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

PA_FINAL = spark.sql(' \
                        SELECT DISTINCT b.value as src_sentence, c.value as tgt_sentence \
                        FROM CLEAN_INPUT_FILE_DF a, EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE hash(a.sentence) = hash(b.value) AND b.row_id = c.row_id')



# #########################################################
# 9. Load TELUGU 
# #########################################################
EN_INPUT_FILE="/user/root/parallel-sentences/te/source_en_ik_06_02_20.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

TGT_INPUT_FILE="/user/root/parallel-sentences/te/target_te_ik_06_02_20.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

TE_FINAL = spark.sql(' \
                        SELECT DISTINCT b.value as src_sentence, c.value as tgt_sentence \
                        FROM CLEAN_INPUT_FILE_DF a, EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE hash(a.sentence) = hash(b.value) AND b.row_id = c.row_id')


# #########################################################
# 10. Load URDU 
# #########################################################
EN_INPUT_FILE="/user/root/parallel-sentences/ur/source_en_ik_06_02_20.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

TGT_INPUT_FILE="/user/root/parallel-sentences/ur/target_ur_ik_06_02_20.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

UR_FINAL = spark.sql(' \
                        SELECT DISTINCT b.value as src_sentence, c.value as tgt_sentence \
                        FROM CLEAN_INPUT_FILE_DF a, EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE hash(a.sentence) = hash(b.value) AND b.row_id = c.row_id')

TA_FINAL.createOrReplaceTempView('TA_FINAL')
HI_FINAL.createOrReplaceTempView('HI_FINAL')
BN_FINAL.createOrReplaceTempView('BN_FINAL')
GU_FINAL.createOrReplaceTempView('GU_FINAL')
KA_FINAL.createOrReplaceTempView('KA_FINAL')
ML_FINAL.createOrReplaceTempView('ML_FINAL')
MR_FINAL.createOrReplaceTempView('MR_FINAL')
PA_FINAL.createOrReplaceTempView('PA_FINAL')
TE_FINAL.createOrReplaceTempView('TE_FINAL')
UR_FINAL.createOrReplaceTempView('UR_FINAL')


# ##########
# TESTING 1
# ##########
FINAL = spark.sql(' \
                        SELECT a.original_sent as pdf_sentence, \
                               hi.tgt_sentence as hi \
                        FROM \
                            CLEAN_INPUT_FILE_DF a \
                            LEFT OUTER JOIN HI_FINAL hi \
                        ON hash(a.sentence) = hash(hi.src_sentence) \
                ')

# ##########
# TESTING 2
# ##########
FINAL = spark.sql(' \
                        SELECT a.original_sent as pdf_sentence, \
                               ta.tgt_sentence as ta, \
                               hi.tgt_sentence as hi \
                        FROM \
                            CLEAN_INPUT_FILE_DF a \
                            LEFT OUTER JOIN TA_FINAL ta \
                        ON hash(a.sentence) = hash(ta.src_sentence) \
                            LEFT OUTER JOIN HI_FINAL hi \
                        ON hash(a.sentence) = hash(hi.src_sentence) \
                ')
# ##########

FINAL = spark.sql(' \
                        SELECT a.original_sent as pdf_sentence, \
                               ta.tgt_sentence as ta, \
                               hi.tgt_sentence as hi, \
                               bn.tgt_sentence as bn, \
                               gu.tgt_sentence as gu, \
                               ka.tgt_sentence as ka, \
                               ml.tgt_sentence as ml, \
                               mr.tgt_sentence as mr, \
                               pa.tgt_sentence as pa, \
                               te.tgt_sentence as te, \
                               ur.tgt_sentence as ur \
                        FROM \
                            CLEAN_INPUT_FILE_DF a \
                            LEFT OUTER JOIN TA_FINAL ta \
                        ON hash(a.sentence) = hash(ta.src_sentence) \
                            LEFT OUTER JOIN HI_FINAL hi \
                        ON hash(a.sentence) = hash(hi.src_sentence) \
                            LEFT OUTER JOIN BN_FINAL bn \
                        ON hash(a.sentence) = hash(bn.src_sentence) \
                            LEFT OUTER JOIN GU_FINAL gu \
                        ON hash(a.sentence) = hash(gu.src_sentence) \
                            LEFT OUTER JOIN KA_FINAL ka \
                        ON hash(a.sentence) = hash(ka.src_sentence) \
                            LEFT OUTER JOIN ML_FINAL ml \
                        ON hash(a.sentence) = hash(ml.src_sentence) \
                            LEFT OUTER JOIN MR_FINAL mr \
                        ON hash(a.sentence) = hash(mr.src_sentence) \
                            LEFT OUTER JOIN PA_FINAL pa \
                        ON hash(a.sentence) = hash(pa.src_sentence) \
                            LEFT OUTER JOIN TE_FINAL te \
                        ON hash(a.sentence) = hash(te.src_sentence) \
                            LEFT OUTER JOIN UR_FINAL ur \
                        ON hash(a.sentence) = hash(ur.src_sentence) \
                ')

FINAL.write.csv('/user/root/final-all-lang1')






# ########################################################################################
# LOCAL TESTING
# ########################################################################################



from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .appName("All_Pairs_Sentences") \
    .enableHiveSupport() \
    .getOrCreate()


# Read clean sentences (215k records)
SCHEMA_CLEAN_INPUT_FILE = StructType([
                             StructField("original_sent", StringType(), True),
                             StructField("sentence_dup", StringType(), True),
                             StructField("hash_val", StringType(), True),
                             StructField("pdf_filename", StringType(), True),
                             ])

CLEAN_INPUT_FILE="/Users/TIMAC044/Documents/Anuvaad/parallel-sentences.csv"
CLEAN_INPUT_FILE_DF = spark.read.format("com.databricks.spark.csv") \
                  .option("delimiter", ',') \
                  .option("quote", "\"") \
                  .option("escape", "\"") \
                  .option("header", "false") \
                  .schema(SCHEMA_CLEAN_INPUT_FILE) \
                  .load(CLEAN_INPUT_FILE)

CLEAN_INPUT_FILE_DF = CLEAN_INPUT_FILE_DF.select("original_sent") \
                   .withColumn("sentence", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))

CLEAN_INPUT_FILE_DF.createOrReplaceTempView('CLEAN_INPUT_FILE_DF')



# #########################################################
# 1. Load TAMIL 
# #########################################################
EN_INPUT_FILE="/Users/TIMAC044/Documents/parallel-sentences/ur/source_en_ik_06_02_20.txt"
EN_FILE_DF = spark.sparkContext.textFile(EN_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("original_sent"), F.col("_2").alias("row_id")).withColumn("value", F.regexp_replace(F.lower(F.col('original_sent')), '[^a-zA-Z0-9 ]+', ''))
EN_FILE_DF.createOrReplaceTempView('EN_FILE_DF')

TGT_INPUT_FILE="/Users/TIMAC044/Documents/parallel-sentences/ur/target_ur_ik_06_02_20.txt"
TGT_FILE_DF = spark.sparkContext.textFile(TGT_INPUT_FILE).zipWithIndex().toDF().select(F.col("_1").alias("value"), F.col("_2").alias("row_id"))
TGT_FILE_DF.createOrReplaceTempView('TGT_FILE_DF')

TA_FINAL = spark.sql(' \
                        SELECT DISTINCT b.original_sent as src_sentence, c.value as tgt_sentence \
                        FROM EN_FILE_DF b, TGT_FILE_DF c \
                        WHERE b.row_id = c.row_id')

TA_FINAL.createOrReplaceTempView('TA_FINAL')

FINAL = spark.sql(' \
                        SELECT DISTINCT \
                            a.original_sent as pdf_sentence, \
                            tgt.tgt_sentence \
                        FROM \
                            CLEAN_INPUT_FILE_DF a \
                        JOIN \
                            TA_FINAL tgt \
                        ON \
                            hash(a.original_sent) = hash(tgt.src_sentence) \
                ')

FINAL.write.csv('/tmp/ur-overlap')



