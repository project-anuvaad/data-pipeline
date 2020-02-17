# -*- coding: utf-8 -*-

######################################################
# PROJECT : Anuvaad Token Extractor
# AUTHOR  : Tarento Technologies
# DATE    : Jan 17, 2020
######################################################

import json
import re
import sys
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.sql.types import StructType, StructField, ArrayType, StringType

'''
This is the scalable framework for the Token extraction process for Anuvaad pipeline.
The code reads the input file and generates the negative tokens based on the specified
rules. This in turn would be used for generating the valid tokenized sentences in the 
next stage. The user can manually verify the generated negative token before kicking 
off the next stage.

Please read the general documentation for Anuvaad before going through the code.

The code has been tested on HDP 3.1.4 and Airflow 1.10.7 stack.

'''


'''
---------------------------------------
SPARK SESSION CREATION
---------------------------------------
'''
spark = SparkSession \
    .builder \
    .appName("Anuvaad_Sentence_Extractor") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

LOG4JLOGGER = spark.sparkContext._jvm.org.apache.log4j
LOGGER = LOG4JLOGGER.LogManager.getLogger(__name__)
LOGGER.info("====================================================")
LOGGER.info("Starting the Token Extraction process for Anuvaad...")
LOGGER.info("====================================================")


'''
---------------------------------------
LOAD CONFIG FILE
---------------------------------------
'''

LOGGER.info("Following config passed to the Driver :")
LOGGER.info(sys.argv[1])
ANUVAAD_INPUT_CONFIG   = json.loads(sys.argv[1])

# EXTRACT THE VALUES FROM THE CONFIG
IN_NEG_TOKENS_SET  = ANUVAAD_INPUT_CONFIG["add_negative_tokens"]
IN_TKN_LEN_MIN     = ANUVAAD_INPUT_CONFIG["token_length_min"]
IN_TKN_LEN_MAX     = ANUVAAD_INPUT_CONFIG["token_length_max"]
IN_REGEX_RULES     = ANUVAAD_INPUT_CONFIG["regex_rules_for_token_extraction"]
#IN_INPUT_FILE     = "/user/root/anuvaad/input/files/0000000485.csv"
IN_INPUT_FILE      = ANUVAAD_INPUT_CONFIG["input_file"]
#IN_DEFAULT_TOKEN_FILE = "/user/root/anuvaad/input/default-tokens/default_tokens.csv"
IN_DEFAULT_TOKEN_FILE = ANUVAAD_INPUT_CONFIG["default_tokens"]
#OUT_NEG_TOKENS    = "/user/root/anuvaad/intermediate/neg-tokens"
OUT_NEG_TOKENS     = ANUVAAD_INPUT_CONFIG["output_neg_tokens"]

def extract_tokens(x):
    all_tokens = []
    for rule in IN_REGEX_RULES:
        tokens = [m.group() for m in re.finditer(rule, x, re.IGNORECASE)]
        all_tokens.extend(tokens)
    return all_tokens


'''
---------------------------------------
HDFS LOADING
---------------------------------------
'''

# READ INPUT FILE
LOGGER.info("Starting to load the input files from HDFS...")

SCHEMA_INPUT_FILE = StructType([StructField("sentences", StringType(), True)])
DF_INPUT_FILE = spark.read.format("com.databricks.spark.csv") \
                  .option("delimiter", ",") \
                  .option("quote", "\"") \
                  .option("escape", "\"") \
                  .option("header", "false") \
                  .option("delimiter", "\n") \
                  .schema(SCHEMA_INPUT_FILE) \
                  .load(IN_INPUT_FILE)
DF_INPUT_FILE_CLEAN    = DF_INPUT_FILE.na.fill("")

LOGGER.info("Successfully loaded the input files from HDFS.")

# READ DEFAULT TOKENS
LOGGER.info("Starting to load the pre-defined Positive Tokens from HDFS...")

SCHEMA_DEFAULT_TOKENS = StructType([StructField("default_token", StringType(), True)])
DF_DEFAULT_NEG_TOKENS = spark.read.format("com.databricks.spark.csv") \
                          .option("delimiter", ",") \
                          .option("quote", "\"") \
                          .option("escape", "\"") \
                          .option("header", "false") \
                          .schema(SCHEMA_DEFAULT_TOKENS) \
                          .load(IN_DEFAULT_TOKEN_FILE)

LOGGER.info("Successfully loaded the pre-defined Positive Tokens from HDFS.")


'''
---------------------------------------
INPUT FILE PROCESSING
---------------------------------------
'''

LOGGER.info("Starting to ETL processing for Token Extraction...")

# TOKENIZE & DEDUP
TOKENIZER_MAPPING = Tokenizer(inputCol="sentences", outputCol="tokens")
DF_TOKENS_DATA    = TOKENIZER_MAPPING.transform(DF_INPUT_FILE_CLEAN)
DF_INPUT          = (DF_TOKENS_DATA.drop("sentences").select(F.explode(F.col("tokens")).alias("token"))).distinct()

FILTER_UDF = F.udf(extract_tokens, ArrayType(StringType()))

# Gets the last character
DF_REGEX_FILTERED = DF_INPUT \
                      .select(F.explode(FILTER_UDF(DF_INPUT.token)).alias("ntoken")) \
                      .distinct() \
                      .withColumn("is_last_char_period", F.when(F.substring(F.col("ntoken"), -1, 1) == '.', True).otherwise(False))

# Also Remove the trailing '.'
DF_LEN_FILTER = DF_REGEX_FILTERED \
                  .filter( \
                    (F.col("is_last_char_period")) & \
                    (F.length(F.col("ntoken")) > IN_TKN_LEN_MIN) & \
                    (F.length(F.col("ntoken")) < IN_TKN_LEN_MAX)) \
                  .select(F.expr("substring(ntoken, 1, length(ntoken)-1)").alias('actual_token'))

DF_LEN_FILTER_EXTEND = DF_LEN_FILTER \
                         .filter( \
                           (F.col("actual_token").like('%.%')) | \
                           ((~ F.col("actual_token").like('%.%')) & (F.length(F.col("actual_token")) < 4)))


'''
---------------------------------------
DEFAULT TOKEN PROCESSING
---------------------------------------
'''

# READ EXTERNAL NEG TOKENS
DF_EXTERNAL_NEG_TOKENS = spark.createDataFrame(IN_NEG_TOKENS_SET, StringType()).toDF("neg_tokens")

DF_FINAL_NEG_TOKENS = DF_LEN_FILTER_EXTEND.union(DF_EXTERNAL_NEG_TOKENS).subtract(DF_DEFAULT_NEG_TOKENS)

# Consolidate & write the output into a single file.
DF_FINAL_NEG_TOKENS.coalesce(1).write \
                                 .format("text") \
                                 .option("header", "false") \
                                 .mode("overwrite") \
                                 .save(OUT_NEG_TOKENS)

LOGGER.info("Successfully completed the ETL processing for Token Extraction.")
