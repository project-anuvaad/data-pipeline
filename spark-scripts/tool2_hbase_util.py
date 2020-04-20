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
hbase_server = "sandbox-hdp.hortonworks.com"
hbase_table = "translated_sentences"


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
        
        source_sentence = source_sentence
        target_sentence = target_sentence.encode('UTF-8','strict')
        encoded_str = hashlib.sha256(source_sentence.encode())
        hash_hex = encoded_str.hexdigest()
        row_key=target_language+"_"+hash_hex
        print(row_key) 
        table.put(row_key, {"info:source_sentence": source_sentence})
        table.put(row_key, {"info:target_sentence": target_sentence})
        table.put(row_key, {"info:source_language": source_language})
        table.put(row_key, {"info:target_language": target_language})        

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
        source_sentence = source_sentence
        encoded_str = hashlib.sha256(source_sentence.encode())
        hash_hex = encoded_str.hexdigest()
        print(hash_hex)


        row_key=target_language+"_"+hash_hex
        
        print(row_key) 
        row = table.row(row_key)
        if row:      
           translated_sentence = row[b'info:target_sentence'].decode('UTF-8','strict')
        else:
           translated_sentence=""
        return translated_sentence
    except Exception as e:
        print('get_translation : Error occurred in getting the translation '', error is == ' + str(e))