# -*- coding: UTF-8 -*-
#
######################################################
# PROJECT : MongoDB-Spark Connector
# AUTHOR  : Tarento Technologies
# DATE    : Oct 09, 2020
######################################################

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

'''
This is the simple code for PySpark MongoDB connection.

The code has 2 parts: 
 - Write a Spark DF to MongoDB
 - Read a MongoDB collection into a Spark DF.

Starting from pyspark :
~~~~~~~~~~~~~~~~~~~~~~~~
pyspark --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/anuvaad.testpeople?readPreference=primaryPreferred" \
        --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/anuvaad.testpeople" \
        --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.0

Submit using spark-submit :
~~~~~~~~~~~~~~~~~~~~~~~~~~~
spark-submit --master "local[4]"  \
        --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/anuvaad.testpeople?readPreference=primaryPreferred" \
        --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/anuvaad.testpeople" \
        --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 \
        mongodb_read_write_spark.py

'''
def main():
    spark = SparkSession.builder\
                    .master('local')\
                    .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/anuvaad.testpeople') \
                    .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/anuvaad.testpeople') \
                    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11-2.4.0') \
                    .getOrCreate()

    # WRITE TO MONGODB
    testpeople = spark.createDataFrame([("JULIA", "50"),
                                        ("Gandalf", "1000"),
                                        ("Thorin", "195"),
                                        ("Balin", "178"),
                                        ("Kili", "77"),
                                        ("Dwalin", "169"),
                                        ("Oin", "167"),
                                        ("Gloin", "158"),
                                        ("Fili", "82"),
                                        ("Bombur", "22")
                                    ], ["firstname", "lastname"])

    testpeople.write\
          .format("com.mongodb.spark.sql.DefaultSource")\
          .mode("append")\
          .option("database", "anuvaad")\
          .option("collection", "testpeople")\
          .save()


    # READ FROM MONGODB
    df = spark.read\
          .format("com.mongodb.spark.sql.DefaultSource")\
          .option("database", "anuvaad")\
          .option("collection", "testpeople")\
          .load()

    df.show()

    df.select('*').where(col("firstname") == "JULIA").show()


    # UPDATE MONGODB
    upd_id = "5f7fec1648f90e8313281c74"
    df.filter(df._id.oid == upd_id) .show()
    
    schema = StructType().add("_id", StructType().add("oid", StringType())) \
                         .add("firstname", StringType()) \
                         .add("lastname", StringType())
    
    df1 = spark.createDataFrame(
        [
            {
              "_id": {
                 "oid": upd_id
              },
              "firstname" : "Aravinth_Updated",
              "lastname" : "Bheemaraj_Updated",
            }
        ],
        schema
    )
    
    df1.write\
          .format("com.mongodb.spark.sql.DefaultSource")\
          .mode("append")\
          .option("database", "anuvaad")\
          .option("collection", "testpeople")\
          .option("replaceDocument", "false")\
          .save()


if __name__ == "__main__":
    main()