# -*- coding: utf-8 -*-

######################################################
# PROJECT : Anuvaad Sentence Extractor
# AUTHOR  : Tarento Technologies
# DATE    : Jan 24, 2020
######################################################

import nltk
import pyspark.sql.functions as F
from pyspark import AccumulatorParam
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType


'''
This is the scalable framework for the Sentence extraction process for Anuvaad pipeline.
The code reads the output of the stage 1 (Negative token extraction step). 
The identified negative tokens is used for generating the valid tokenized sentences here. 

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
LOGGER.info("=======================================================")
LOGGER.info("Starting the Sentence Extraction process for Anuvaad...")
LOGGER.info("=======================================================")


'''
---------------------------------------
VARIABLES
---------------------------------------
'''

PICKLE_DOWNLOAD_DIR = '/home/anuvaad/'
#IN_INPUT_FILE      = "/user/root/anuvaad/input/files/0000000485.csv"
IN_INPUT_FILE       = ANUVAAD_INPUT_CONFIG["input_file"]
#IN_NEG_TOKENS      = "/user/root/anuvaad/intermediate/neg-tokens" + "/*.txt"
IN_NEG_TOKENS       = ANUVAAD_INPUT_CONFIG["output_neg_tokens"] + "/*.txt"
#OUT_SENTENCES      = "/user/root/anuvaad/output/tokenized-sentences"
OUT_SENTENCES       = ANUVAAD_INPUT_CONFIG["output_sentences"]

LOGGER.info("Downloading the English pickle..")
nltk.download('punkt', download_dir=PICKLE_DOWNLOAD_DIR)
stokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
LOGGER.info("Successfully downloaded the English pickle.")


class NegativeTokenAccumulator(AccumulatorParam):
    def zero(self, initialValue):
        return initialValue.copy()
    def addInPlace(self, v1, v2):
        return v1.union(v2)


neg_tokens = spark.sparkContext.accumulator(set(), NegativeTokenAccumulator())

def add_neg_tokens(x):
    global neg_tokens
    neg_tokens += {x}
    return x


'''
---------------------------------------
HDFS LOADING
---------------------------------------
'''

# READ INPUT FILE
SCHEMA_INPUT_FILE = StructType([StructField("sentences", StringType(), True)])
DF_INPUT_FILE     = spark.read.format("com.databricks.spark.csv") \
                      .option("delimiter" ,",") \
                      .option("quote", "\"") \
                      .option("escape", "\"") \
                      .option("header", "false") \
                      .option("delimiter", "\n") \
                      .schema(SCHEMA_INPUT_FILE) \
                      .load(IN_INPUT_FILE)
DF_INPUT_FILE_CLEAN= DF_INPUT_FILE.na.fill("")


# READ GENERATED NEGATIVE TOKENS
SCHEMA_NEG_TOKENS   = StructType([StructField("neg_token", StringType(), True)])
DF_FINAL_NEG_TOKENS = spark.read.format("com.databricks.spark.csv") \
                        .option("delimiter", ",") \
                        .option("quote", "\"") \
                        .option("escape", "\"") \
                        .option("header", "false") \
                        .schema(SCHEMA_NEG_TOKENS) \
                        .load(IN_NEG_TOKENS)


'''
---------------------------------------
SENTENCE TOKENIZATION ETL
---------------------------------------
'''

LOGGER.info("Starting to load all the identified Negative Tokens...")

DF_FINAL_NEG_TOKENS.rdd.foreach(add_neg_tokens)
for t in neg_tokens.value:
    stokenizer._params.abbrev_types.add(t)

LOGGER.info("Successfully loaded all the identified Negative Tokens.")
# LOGGER.info(stokenizer._params.abbrev_types)


LOGGER.info("Starting to apply the tokenization regex for Sentence Extraction...")
sent_udf               = F.udf(lambda x: stokenizer.tokenize(x), ArrayType(StringType()))

TOKENIZED_SENTENCES    = DF_INPUT_FILE_CLEAN.withColumn('tokenized_sents', sent_udf(DF_INPUT_FILE_CLEAN.sentences))

CLEANED_TOKENIZED_SENT = TOKENIZED_SENTENCES \
                             .drop('sentences') \
                             .select(F.explode(F.col('tokenized_sents'))) \
                             .distinct()

CLEANED_TOKENIZED_SENT.coalesce(1).write \
                                 .format("text") \
                                 .option("header", "false") \
                                 .mode("overwrite") \
                                 .save(OUT_SENTENCES)

LOGGER.info("Successfully extracted the Sentences.")
