# -*- coding: utf-8 -*-
######################################################
# PROJECT : Anuvaad Sentence Translator
# AUTHOR  : Tarento Technologies
# DATE    : MAR 30, 2020
######################################################



import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType,StringType
from googletrans import Translator
import hbase_spark as hbase
import csv
import requests
import json
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import regexp_extract, input_file_name
from ruamel import yaml

'''
------------------------------------------------------
Intialalize Spark Context Google Translator and LOGGER
------------------------------------------------------
'''
translator = Translator()
conf = SparkConf().setAppName("spark-test").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config('spark.executor.memory', '1g').getOrCreate()
LOG4JLOGGER = spark.sparkContext._jvm.org.apache.log4j
LOGGER = LOG4JLOGGER.LogManager.getLogger(__name__)
LOGGER.info(sc.getConf().getAll())

'''
---------------------------------------
LOAD CONFIG FILE
---------------------------------------
'''
LOGGER.info("Following config passed to the Driver :")

ANUVAAD_INPUT_CONFIG= yaml.safe_load(open('config.yml'))

'''
---------------------------------------
Get config values from yml file
---------------------------------------
'''
url = ANUVAAD_INPUT_CONFIG["url"]
source_language = ANUVAAD_INPUT_CONFIG["source_language"]
target_language = ANUVAAD_INPUT_CONFIG["target_language"]
input_file_folder = ANUVAAD_INPUT_CONFIG["source_folder"]
regex_pattern = ANUVAAD_INPUT_CONFIG["regex_pattern"]
output_file_folder = ANUVAAD_INPUT_CONFIG["target_folder"]

'''
-------------------------------------------------------
Translator function using Tarento model translation API
-------------------------------------------------------
'''
def get_model_translation(word):
    body=[{'src': word, 'id': 56 }]
    response = requests.post(url,json=body)
    data = response.json()
    tgt = data['response_body'][0]['tgt']
    return tgt



LOGGER.info('=======================================================')
LOGGER.info('Starting the Sentence Translation process for Anuvaad...')
LOGGER.info('=======================================================')

'''
------------------------------------------------------
Import the hbase-integration module
------------------------------------------------------
'''
sc.addFile("hbase_spark.py")

'''
------------------------------------------------------
Parse all the lines and translate the sentences
------------------------------------------------------
'''
def parseLine(line):
    
    target = hbase.get_translation(line[0],source_language,target_language)
    if target:
        print('Translated text from HBASE')
    else:
        print('Translation not found')
        target = get_model_translation(line[0])
        hbase.store_translation(line[0],target,source_language,target_language)
    return (line[0], target,line[1])

'''
------------------------------------------------------
Input File Structure
------------------------------------------------------
'''

SCHEMA_INPUT_FILE = StructType([StructField('sentences', StringType(),
                               True)])

'''
------------------------------------------------------
Parse all the input files and convert the data to data frame
------------------------------------------------------
'''

DF_INPUT_FILE = spark.read.format('com.databricks.spark.csv'
                                  ).option('escape', '"'
        ).option('header', 'false').option('delimiter', ','
        ).option('quote', '"').option('encoding', 'cp1252'
        ).schema(SCHEMA_INPUT_FILE).load(input_file_folder)


df2 = DF_INPUT_FILE.withColumn("file_name", F.split(regexp_extract(input_file_name(),regex_pattern,1),'/')[1])
df2.show()
parsedLines = df2.rdd.map(parseLine)
df = spark.createDataFrame(parsedLines).toDF("source_sentence", "target_sentence","file_name")

'''
---------------------------------------------------------------------
Generate the parallel corpus files partitioned by the input file name
---------------------------------------------------------------------
'''
df.coalesce(1).write.option("encoding", "utf-8").mode("overwrite").format("com.databricks.spark.csv").partitionBy("file_name").save(output_file_folder)

LOGGER.info('Successfully translated the Sentences.')




