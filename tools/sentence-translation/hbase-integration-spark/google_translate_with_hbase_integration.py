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
import hbase_utils as hbase
import csv
import requests
import json
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import regexp_extract, input_file_name
import yaml
import subprocess
from pyspark.sql.functions import udf
'''
------------------------------------------------------
Function to execute hadoop commands
------------------------------------------------------
'''

def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    (output, errors) = proc.communicate()
    if proc.returncode:
        raise RuntimeError(
            'Error running command: %s. Return code: %d, Error: %s' % (
                ' '.join(args_list), proc.returncode, errors))
    return (output, errors)

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

#ANUVAAD_INPUT_CONFIG= yaml.safe_load(open('config.yml'))

'''
---------------------------------------
Get config values from yml file
---------------------------------------
'''
# url = ANUVAAD_INPUT_CONFIG["url"]
# source_language = ANUVAAD_INPUT_CONFIG["source_language"]
# target_language = ANUVAAD_INPUT_CONFIG["target_language"]
# input_file_folder = ANUVAAD_INPUT_CONFIG["source_folder"]
# regex_pattern = ANUVAAD_INPUT_CONFIG["regex_pattern"]
# output_file_folder = ANUVAAD_INPUT_CONFIG["target_folder"]

#url = "http://52.40.71.62:3003/translator/translate-anuvaad"
url = "https://translation.googleapis.com/language/translate/v2"
source_language = "en"
target_language = "hi"
input_file_folder = "/user/root/source_files/*.csv"
regex_pattern = "[\/]([^\/]+[\/][^\/]+)$"
output_file_folder = "/user/root/translated"
headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer XXXXX',
}

req_data_template = """
{
  'q': 'QUERY',
  'source': 'en',
  'target': 'hi',
  'format': 'text'
}
"""


'''
-------------------------------------------------------
Translator function using Tarento model translation API
-------------------------------------------------------
'''
def get_model_translation(word):
    #body=[{'src': word, 'id': 56 }]
    #id:56 for en-hi
    #response = requests.post(url,json=body)
    req_data = req_data_template.replace('QUERY', word)
    response = requests.post(url, headers=headers, data=req_data)
    data = response.json()
    #tgt = data['response_body'][0]['tgt']
    tgt = data['data']['translations'][0]['translatedText']
    return tgt



LOGGER.info('=======================================================')
LOGGER.info('Starting the Sentence Translation process for Anuvaad...')
LOGGER.info('=======================================================')

'''
------------------------------------------------------
Import the hbase-integration module
------------------------------------------------------
'''
sc.addFile("hbase_utils.py")

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
Move File Function
------------------------------------------------------
'''
def fileName(filename):

    print('coming in to the function')
    file_name = filename["file_name"]
    return file_name
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
        ).option('quote', '"').option('encoding', 'utf-8'
        ).schema(SCHEMA_INPUT_FILE).load(input_file_folder )


df2 = DF_INPUT_FILE.withColumn("file_name", F.split(regexp_extract(input_file_name(),regex_pattern,1),'/')[1])

parsedLines = df2.rdd.map(parseLine)
df = spark.createDataFrame(parsedLines).toDF("source_sentence", "target_sentence","file_name")
#print('After this line the map function is repeating')


'''
---------------------------------------------------------------------
Generate the parallel corpus files partitioned by the input file name
---------------------------------------------------------------------
'''
df.coalesce(1).write.option("encoding", "utf-8").mode("overwrite").format("com.databricks.spark.csv").partitionBy("file_name").save(output_file_folder)

'''
---------------------------------------------------------------------
Copy Files in to a proper folder. Function Disabled
---------------------------------------------------------------------
'''
#df_filename = df.select("file_name").distinct()
#out = df_filename.rdd.map(fileName)


# for filename in df_filename.collect():
  # fname = filename[0].split('.')[0]
  # print(fname)
  # source = output_file_folder+"/file_name="+filename[0]+"/*.csv"
  # destination = output_file_folder+"/"+fname+"_translated_"+source_language+"_"+target_language+".csv"
  # (out, errors)= run_cmd(['hadoop', 'fs', '-cp', '-f', source, destination])

LOGGER.info('Successfully translated the Sentences.')
