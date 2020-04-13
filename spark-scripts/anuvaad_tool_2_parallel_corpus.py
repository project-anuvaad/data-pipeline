#!/usr/bin/python
# -*- coding: utf-8 -*-
######################################################
# PROJECT : Anuvaad Sentence Translator
# AUTHOR  : Tarento Technologies
# DATE    : MAR 30, 2020
######################################################



import pyspark.sql.functions as F
from pyspark import AccumulatorParam
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, \
    StringType
from googletrans import Translator
import tool2_hbase_util as hbase
import csv
import requests
import json
from pyspark import SparkConf, SparkContext



def get_model_translation(word):
    
    url = "http://52.40.71.62:3003/translator/translate-anuvaad"
    body=[{'src': word, 'id': 56 }]
    response = requests.post(url,json=body)
    data = response.json()
    tgt = data['response_body'][0]['tgt']
    return tgt


translator = Translator()
conf = SparkConf().setAppName("spark-test").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config('spark.executor.memory', '1g').getOrCreate()

LOG4JLOGGER = spark.sparkContext._jvm.org.apache.log4j
LOGGER = LOG4JLOGGER.LogManager.getLogger(__name__)
LOGGER.info('=======================================================')
LOGGER.info('Starting the Sentence Translation process for Anuvaad...')
LOGGER.info('=======================================================')
sc.addFile("tool2_hbase_util.py")

# READ INPUT FILE
def parseLine(line):
    
    source = line
    target = hbase.get_translation(source[0],"en","hi")
    if target:
        print('Translated text from HBASE')
    else:
        print('Translation not found')
        target = get_model_translation(source[0])
        hbase.store_translation(source[0],target,"en","hi")
    return (source[0], target)


SCHEMA_INPUT_FILE = StructType([StructField('sentences', StringType(),
                               True)])
DF_INPUT_FILE = spark.read.format('com.databricks.spark.csv'
                                  ).option('escape', '"'
        ).option('header', 'false').option('delimiter', ','
        ).option('quote', '"').option('encoding', 'cp1252'
        ).schema(SCHEMA_INPUT_FILE).load('*.csv')
parsedLines = DF_INPUT_FILE.rdd.map(parseLine)
df = spark.createDataFrame(parsedLines)

#df.show()

df.coalesce(1).write.option("encoding", "utf-8").mode("overwrite").format("com.databricks.spark.csv").save("/user/root/translated/en_hi_v2.csv")

LOGGER.info('Successfully translated the Sentences.')




