# -*- coding: utf-8 -*-

######################################################
# PROJECT : Sentence Similarity Calculator
# AUTHOR  : Tarento Technologies
# DATE    : May 05, 2020
######################################################


from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, Model, PipelineModel
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH

from pyspark.sql.types import *
from pyspark.sql.functions import col

'''
This is the scalable ML based code for identifying the sentence similarity
between two datasets. Algorithm used is MinHashLSH.

'''


'''
---------------------------------------
SPARK SESSION CREATION
---------------------------------------
'''
spark = SparkSession \
    .builder \
    .appName("Sentence_Similarity_Tool") \
    .getOrCreate()


spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.executor.cores", 6)
spark.conf.set("spark.dynamicAllocation.minExecutors","3")
spark.conf.set("spark.dynamicAllocation.maxExecutors","6")


LOG4JLOGGER = spark.sparkContext._jvm.org.apache.log4j
LOGGER = LOG4JLOGGER.LogManager.getLogger(__name__)
LOGGER.info("-------------------------------------------------------")
LOGGER.info("Starting the Sentence Similarity Identifier...")
LOGGER.info("-------------------------------------------------------")

# Define the Inputs
#IN_INPUT_FILE = "/Users/TIMAC044/Documents/Anuvaad/sentences_comparison/indian_kanoon_scrapped_sentences.csv"
IN_INPUT_FILE="/user/root/sim-score/input/scrapped_sentences_full/ik_data_final.csv"
#PDF_INPUT_FILE = "/Users/TIMAC044/Documents/Anuvaad/sentences_comparison/sample_indian_kanoon_twisted_sentence_sample.csv"
PDF_INPUT_FILE = "/user/root/sim-score/input/sc_tokenized_sentences/sent.out"

# Schema of the files
SCHEMA_INPUT_FILE = StructType([StructField("sentences", StringType(), True)])


SCRAPPED_INPUT_FILE = spark.read.format("com.databricks.spark.csv") \
                  .option("delimiter", ",") \
                  .option("quote", "\"") \
                  .option("escape", "\"") \
                  .option("header", "false") \
                  .option("delimiter", "\n") \
                  .schema(SCHEMA_INPUT_FILE) \
                  .load(IN_INPUT_FILE)
DF_INPUT_FILE_CLEAN    = SCRAPPED_INPUT_FILE.filter("sentences rlike '[A-Z,a-z]'").repartition(100)


DOWNLOAD_PDF_INPUT_FILE = spark.read.format("com.databricks.spark.csv") \
                  .option("delimiter", ",") \
                  .option("quote", "\"") \
                  .option("escape", "\"") \
                  .option("header", "false") \
                  .option("delimiter", "\n") \
                  .schema(SCHEMA_INPUT_FILE) \
                  .load(PDF_INPUT_FILE)
DOWNLOAD_PDF_INPUT_FILE_CLEAN    = DOWNLOAD_PDF_INPUT_FILE.filter("sentences rlike '[A-Z,a-z]'").repartition(100)

LOGGER.info("-------------------------------------------------------")
LOGGER.info("Loaded the dataframes...")
LOGGER.info("-------------------------------------------------------")

pipeline = Pipeline(stages=[
    RegexTokenizer(
        pattern="", inputCol="sentences", outputCol="tokens", minTokenLength=1
    ),
    NGram(n=2, inputCol="tokens", outputCol="ngrams"),
    HashingTF(inputCol="ngrams", outputCol="vectors"),
    MinHashLSH(inputCol="vectors", outputCol="lsh")
])

model         = pipeline.fit(DF_INPUT_FILE_CLEAN)
stored_hashed = model.transform(DF_INPUT_FILE_CLEAN)
landed_hashed = model.transform(DOWNLOAD_PDF_INPUT_FILE_CLEAN)
matched_df    = model.stages[-1].approxSimilarityJoin(stored_hashed, landed_hashed, 0.5, "confidence")
                     .select(col("datasetA.sentences").alias("ik_sentence"), 
                             col("datasetB.sentences").alias("pdf_sentence"), 
                             col("confidence"))

LOGGER.info("-------------------------------------------------------")
LOGGER.info("Completed the ML pipeline...")
LOGGER.info("-------------------------------------------------------")

# matched_df.filter("confidence < 0.3").show(20, False)
matched_df.coalesce(1).write \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("/user/root/sim-score/output/")

LOGGER.info("-------------------------------------------------------")
LOGGER.info("Sentence Similarity File generated succesfully!")
LOGGER.info("-------------------------------------------------------")
