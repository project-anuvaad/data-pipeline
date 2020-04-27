# -*- coding: utf-8 -*-

######################################################
# PROJECT : Anuvaad PDF Paragraph Parser
# AUTHOR  : Tarento Technologies
# DATE    : Apr 27, 2020
######################################################


import ast
import cv2
import io
import numpy
import os
import re

import pyspark.sql.functions as F
import pyspark.sql.types as T

from PIL import Image
from bs4 import BeautifulSoup

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, MapType, StringType, StructType, StructField



'''
This is the scalable framework for the paragraph extraction process from PDF for 
Anuvaad pipeline. The code reads the input pdf files and converts into HTML files 
pagewise and also generates respective images containing the lines which acts as
the background images for the HTML pages.

There is an external dependency on pdftohtml tool.

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
    .appName("Anuvaad_Pdf_Para_Extractor") \
    .getOrCreate()

LOG4JLOGGER = spark.sparkContext._jvm.org.apache.log4j
LOGGER = LOG4JLOGGER.LogManager.getLogger(__name__)
LOGGER.info("========================================================")
LOGGER.info("Starting the Paragraph Extraction process for Anuvaad...")
LOGGER.info("========================================================")


'''
---------------------------------------
VARIABLES
---------------------------------------
'''

CONVERTED_OUT_DIR = "/tmp/html_out/out"
INPUT_HTML_DOCS = CONVERTED_OUT_DIR + "-*.html"
INPUT_IMAGES = CONVERTED_OUT_DIR + "*.png"
NUM_PARTITIONS = 10


# Define sql Context
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)


# Define String to Array UDF
def parse_array_from_string(x):
    return ast.literal_eval(x)

RETRIEVE_ARRAY_UDF = F.udf(parse_array_from_string, T.ArrayType(T.StringType()))




# Define HTML Parsing UDF
def parse_html_tags(x):
    map = dict()
    para = ''
    soup = BeautifulSoup (x, 'html.parser')
    map['page_no'] = str(soup.title.string)
    # ######################
    # MAP FOR STYLES - Extract Font size, family & color
    # ######################
    style_map = dict()
    for line in soup.find_all("style"):
        for entry in line.text.split():
            style_values = re.search('.(.*){(.*)font-size:([0-9]*)px;(.*)font-family:(.*);(.*)color:(.*);(.*)', entry, re.IGNORECASE)
            if style_values:
                style_map[style_values.group(1)] = (style_values.group(3) + style_values.group(5) + style_values.group(7))
    # ######################
    # print(style_map)
    # ######################
    formatted_text = []
    for line in soup.find_all("p"):
        # Remove unwanted tags
        clean_line = str(line).replace('<br/>','').replace('<i>','').replace('<i/>','')
        # Make the sentence single-spaced and remove leading/trailing spaces
        clean_line = ' '.join(clean_line.split()).strip()
        # Extract the required sections (class, mapped style, p style, actual text)
        para_reg = re.search('<p class="(.*)" style=\"(.*)\">(.*)</p>', clean_line)
        formatted_text += ['{0}\t{1}\t{2}\t{3}'.format(para_reg.group(1), style_map.get(para_reg.group(1)), para_reg.group(2), para_reg.group(3))]
    map['formatted_text'] = str(formatted_text)
    return map

TITLE_UDF = F.udf(parse_html_tags, MapType(StringType(), StringType()))



# Define the UDF to get the line coordinates from an image
def extract_line_coords(binary_images):
    name, img = binary_images
    pil_image = Image.open(io.BytesIO(img)).convert('RGB') 
    cv2_image = numpy.array(pil_image) 
    cv2_image = cv2_image[:, :, ::-1].copy() 
    gray     = cv2.cvtColor(cv2_image, cv2.COLOR_BGR2GRAY)
    MAX_THRESHOLD_VALUE     = 255
    BLOCK_SIZE              = 15
    THRESHOLD_CONSTANT      = 0
    SCALE                   = 15
    # Filter image
    filtered                = cv2.adaptiveThreshold(~gray, MAX_THRESHOLD_VALUE, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, BLOCK_SIZE, THRESHOLD_CONSTANT)
    horizontal              = filtered.copy()
    horizontal_size         = int(horizontal.shape[1] / SCALE)
    horizontal_structure    = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_size, 1))
    # isolate_lines
    cv2.erode(horizontal, horizontal_structure, horizontal, (-1, -1)) # makes white spots smaller
    cv2.dilate(horizontal, horizontal_structure, horizontal, (-1, -1))
    contours                = cv2.findContours(horizontal, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    contours                = contours[0] if len(contours) == 2 else contours[1]
    lines                   = []
    for index, contour in enumerate(contours):
        x, y, w, h = cv2.boundingRect(contour)
        if w > 50:
            lines.append((x,y,w,h))
    return os.path.basename(name), lines




# Convert PDF to HTML 
# NEED TO BE MODIFIED TO INVOKE IN CMD LINE (UNIX)
#pdftohtml -p -c nltk_data/corpora/framenet_v15/docs/book.pdf CONVERTED_OUT_DIR


# Load into RDD
rdd = spark.sparkContext.wholeTextFiles(INPUT_HTML_DOCS, NUM_PARTITIONS)

# Confirm the number of Partitions
LOGGER.info("Number of partitions : + format(rdd.getNumPartitions())")


# glom()- return an RDD created by coalescing all elements within each partition into a list
# print(rdd.glom().collect())

# Convert to DF
SCHEMA_INPUT_FILE = StructType([StructField("filepath", StringType(), True), StructField("html_page_tags", StringType(), True)])
HTML_DF = sqlContext.createDataFrame(rdd, SCHEMA_INPUT_FILE)

#Invoke the UDF to parse out the HTML tags
APPEND_DF = HTML_DF.withColumn("metadata", TITLE_UDF(HTML_DF.html_page_tags))

# Explode to form key-value pairs.
EXPLODED_DF = APPEND_DF.select(APPEND_DF.filepath, F.explode(APPEND_DF.metadata))

# UDF to extract the filename from the File path
FILENAME_EXT_UDF = F.udf(lambda x : os.path.basename(x), StringType())
TRIM_FN_DF = EXPLODED_DF.select(FILENAME_EXT_UDF(EXPLODED_DF.filepath).alias("filename"), EXPLODED_DF.key, EXPLODED_DF.value)


# DEBUG STMTS
###############
FORMATTED_SENT_DF = TRIM_FN_DF.filter("key='formatted_text'").select("value")
# EXPLODED_SENT_DF = FORMATTED_SENT_DF.withColumn("new_col", F.explode(RETRIEVE_ARRAY_UDF(F.col("value"))))
EXPLODED_SENT_DF = FORMATTED_SENT_DF.select(F.explode(RETRIEVE_ARRAY_UDF(F.col("value"))))

split_col = F.split(EXPLODED_SENT_DF['col'], '\t')
# EXPLODED_SENT_DF = EXPLODED_SENT_DF.withColumn('class_val', split_col.getItem(0))
EXPLODED_SENT_DF = EXPLODED_SENT_DF.withColumn('style_key', split_col.getItem(1))
# EXPLODED_SENT_DF = EXPLODED_SENT_DF.withColumn('p_style', split_col.getItem(2))
# EXPLODED_SENT_DF = EXPLODED_SENT_DF.withColumn('p_text', split_col.getItem(3))
# EXPLODED_SENT_DF.show()

MAX_OCCURANCE_DF = EXPLODED_SENT_DF.groupBy("style_key").count().sort(F.desc("count"))
para_style = MAX_OCCURANCE_DF.select('style_key').first().style_key
#print(para_style)

###############


# Define UDF
def parse_para(x, doc_p_style):
    paragraphs_list = []
    end_of_sent_re = """(([\"|”|,|a-zA-Z|0-9|.]{3,}[.|?|!|\"|”|:|;]|([:][ ][-]))$)"""
    lines = parse_array_from_string(x)
    para_text = ''
    for line in lines:
        parts = line.split('\t')
        l_class_val  = parts[0].strip()
        l_style_key  = parts[1].strip()
        l_p_style    = parts[2].strip()
        l_p_text     = parts[3].strip()
        sentence_end = re.search(end_of_sent_re, l_p_text, re.IGNORECASE)
        if(l_style_key != doc_p_style):
            if(para_text.strip() != ''): paragraphs_list += [para_text]
            para_text = ''
        else:
            para_text += l_p_text
            if(sentence_end):
                if(para_text.strip() != ''):
                    if(para_text.strip() != ''): paragraphs_list += [para_text]
                para_text = ''
            else:
                para_text += ' '
    if(para_text.strip() != ''): paragraphs_list += [para_text]
    return paragraphs_list


PARSE_PARA_UDF = F.udf(parse_para, T.ArrayType(T.StringType()))

FINAL_PARA_DF = FORMATTED_SENT_DF.select(F.explode(PARSE_PARA_UDF(F.col("value"), F.lit(para_style))))
FINAL_PARA_DF.count()



# #############################
# Extracting Lines from Images
# #############################
images_rdd = spark.sparkContext.binaryFiles(INPUT_IMAGES)
line_coord_df = images_rdd.map(lambda img: extract_line_coords(img)).toDF()
line_coord_exp_df =line_coord_df.select("_1", F.explode("_2"))
line_coord_exp_df =line_coord_df.select(F.col("_1").alias("filename"), F.explode(F.col("_2")).alias("coord"))
line_coord_exp_df.show(20, False)