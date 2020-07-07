# -*- coding: utf-8 -*-
######################################################
# PROJECT : Anuvaad Sentence Translator
# AUTHOR  : Tarento Technologies
# DATE    : MAR 30, 2020
######################################################

import happybase
import hashlib
from pyspark.sql import SparkSession

'''
------------------------------------------------------
Intialalize Spark and LOGGER
------------------------------------------------------
'''
#spark = SparkSession.builder.config('spark.executor.memory', '1g').getOrCreate()

#LOG4JLOGGER = spark.sparkContext._jvm.org.apache.log4j
#LOGGER = LOG4JLOGGER.LogManager.getLogger(__name__)

'''
---------------------------------------
Get config values from yml file
---------------------------------------
'''
#hbase_server = ANUVAAD_INPUT_CONFIG[hbase_server]
#hbase_table = ANUVAAD_INPUT_CONFIG[hbase_table]
hbase_server = "node1.anuvaad.dev"
hbase_table = "trans_sentences"


'''
---------------------------------------
Connection to Hbase server
---------------------------------------
'''
connection = happybase.Connection(hbase_server)
connection.open()
table = connection.table(hbase_table)

'''
---------------------------------------
Function for Storing the translation
---------------------------------------
'''
def store_translation(source_sentence, target_sentence, source_language, target_language):

    print('store_translation for: '+ source_language + '-' + target_language )
    try:

        source_sentence = source_sentence.encode(encoding="utf-8",errors="strict")
        target_sentence = target_sentence.encode(encoding="utf-8",errors="strict")
        encoded_str = hashlib.sha256(source_sentence)
        hash_hex = encoded_str.hexdigest()
        row_key=target_language+"_"+hash_hex
        print(row_key)
        table.put(row_key, {"cf:source_sentence": source_sentence})
        table.put(row_key, {"cf:target_sentence": target_sentence})
        table.put(row_key, {"cf:source_language": source_language})
        table.put(row_key, {"cf:target_language": target_language})

    except Exception as e:
        print('store_translation : Error occurred in storing the translation '', error is == ' + str(e))
'''
-----------------------------------------------------
Function for getting the translation from HBASE table
-----------------------------------------------------
'''

def get_translation(source_sentence, source_language, target_language):

    print('get_translation for: '+source_language + '-' + target_language )
    try:
        source_sentence = source_sentence.encode(encoding="utf-8",errors="strict")
        encoded_str = hashlib.sha256(source_sentence)
        hash_hex = encoded_str.hexdigest()
        print(hash_hex)


        row_key=target_language+"_"+hash_hex

        print(row_key)
        row = table.row(row_key)
        if row:
           translated_sentence = row[b'cf:target_sentence'].decode(encoding="utf-8",errors="strict")
        else:
           translated_sentence=""
        return translated_sentence
    except Exception as e:
        print('get_translation : Error occurred in getting the translation '', error is == ' + str(e))
