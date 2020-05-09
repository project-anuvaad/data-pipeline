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
from pyspark.sql.types import ArrayType, BooleanType, MapType, StringType, StructType, StructField



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
INPUT_HTML_DOCS   = CONVERTED_OUT_DIR + "-*.html"
INPUT_IMAGES      = CONVERTED_OUT_DIR + "*.png"
NUM_PARTITIONS    = 10
# Define Regex
END_OF_SENT_REGEX = """(([\"|”|,|a-zA-Z|0-9|.]{3,}[.|?|!|\"|”|:|;]|([:][ ][-]))$)"""
STYLE_REGEX       = """.(.*){(.*)font-size:([0-9]*)px;(.*)font-family:(.*);(.*)color:(.*);(.*)"""
TOP_REGEX         = """(.*)(top:)([0-9]*)(.*)"""
LEFT_REGEX        = """(.*)(left:)([0-9]*)(.*)"""


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
    # Extact the numeric value alone
    map['page_no'] = str(soup.title.string).split()[1]
    # ######################
    # MAP FOR STYLES - Extract Font size, family & color
    # ######################
    style_map = dict()
    for line in soup.find_all("style"):
        for entry in line.text.split():
            style_values = re.search(STYLE_REGEX, entry, re.IGNORECASE)
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

PARSE_HTML_TAGS_UDF = F.udf(parse_html_tags, MapType(StringType(), StringType()))



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
APPEND_DF = HTML_DF.withColumn("metadata", PARSE_HTML_TAGS_UDF(HTML_DF.html_page_tags))

# Explode to form key-value pairs.
EXPLODED_DF = APPEND_DF.select(APPEND_DF.filepath, F.explode(APPEND_DF.metadata))

# UDF to extract the filename from the File path
FILENAME_EXT_UDF = F.udf(lambda x : os.path.basename(x), StringType())
TRIM_FN_DF = EXPLODED_DF.select(FILENAME_EXT_UDF(EXPLODED_DF.filepath).alias("filename"), EXPLODED_DF.key, EXPLODED_DF.value)


# Section identifies the style of the paragraph (constant across the entire doc)
FORMATTED_SENT_DF = TRIM_FN_DF.filter("key='formatted_text'").select("value")
EXPLODED_SENT_DF  = FORMATTED_SENT_DF.select(F.explode(RETRIEVE_ARRAY_UDF(F.col("value"))))
split_col         = F.split(EXPLODED_SENT_DF['col'], '\t')
EXPLODED_SENT_DF  = EXPLODED_SENT_DF.withColumn('style_key', split_col.getItem(1))
MAX_OCCURANCE_DF  = EXPLODED_SENT_DF.groupBy("style_key").count().sort(F.desc("count"))
para_style        = MAX_OCCURANCE_DF.select('style_key').first().style_key


# Define UDF
# x is the set of lines containing class, style_key, line style & line text
# doc_p_style is the derived paragraph style for the entire document.
def parse_para(x, doc_p_style):
    out_para_list = []
    # Pick the first style for the entire para
    out_style_list = []
    out_bold_list  = []
    out_y_end_list = []
    out_para_position_list = []
    lines = parse_array_from_string(x)
    para_text    = ''
    para_style   = ''
    prev_top_val = ''
    curr_top_val = ''
    line_index = 0
    for line in lines:
        parts        = line.split('\t')
        l_class_val  = parts[0].strip()
        l_style_key  = parts[1].strip()
        l_p_style    = parts[2].strip()
        l_p_text     = parts[3].strip()
        sentence_end = re.search(END_OF_SENT_REGEX, l_p_text, re.IGNORECASE)
        if(para_text == ''):
            para_style = l_p_style
        # Extract Top Value
        style_values = re.search(TOP_REGEX, l_p_style, re.IGNORECASE)
        if style_values:
            curr_top_val = style_values.group(3)
        # Dummy for now. Fix below :
        out_bold_list += (False,)
        if(l_style_key != doc_p_style):
            # Case : Same line with different styles. Ex : Bold
            if(prev_top_val == curr_top_val):
                para_text += l_p_text
                para_text += ' '
            elif(para_text.strip() != ''):
                out_para_list  += [para_text]
                out_style_list += [para_style]
                out_y_end_list += [curr_top_val]
                out_para_position_list.append("start" if (line_index == 0) else "regular")
                line_index   += 1
                para_text = ''
            prev_top_val = ''
        else:
            para_text += l_p_text
            if(sentence_end):
                if(para_text.strip() != ''):
                    out_para_list  += [para_text]
                    out_style_list += [para_style]
                    out_y_end_list += [curr_top_val]
                    out_para_position_list.append("start" if (line_index == 0) else "regular")
                    line_index   += 1
                para_text = ''
            else:
                para_text += ' '
            prev_top_val = curr_top_val
    # Flush remaining para
    if(para_text.strip() != ''):
        out_para_list  += [para_text]
        out_style_list += [para_style]
        out_y_end_list += [curr_top_val]
        out_para_position_list.append("end_incomplete")
    return out_para_list, out_style_list, out_bold_list, out_y_end_list, out_para_position_list


parse_para_schema = T.StructType([
    T.StructField('para_list', T.ArrayType(T.StringType()), False),
    T.StructField('para_style', T.ArrayType(T.StringType()), False),
    T.StructField('is_bold', T.ArrayType(T.StringType()), False),
    T.StructField('y_end', T.ArrayType(T.StringType()), False),
    T.StructField('para_position', T.ArrayType(T.StringType()), False),
])

PARSE_PARA_UDF = F.udf(parse_para, parse_para_schema)

AGG_PARA_DF = FORMATTED_SENT_DF.select((PARSE_PARA_UDF(F.col("value"), F.lit(para_style))).alias('metrics')).select(F.col('metrics.*'))
AGG_PARA_DF.count()

PARA_WITH_METADATA_DF = AGG_PARA_DF.withColumn("tmp", F.arrays_zip("para_list", "para_style", "is_bold", "y_end", "para_position")) \
                   .withColumn("tmp", F.explode("tmp")) \
                   .select(F.col("tmp.para_list"), \
                           F.col("tmp.para_style"), \
                           F.col("tmp.is_bold"), \
                           F.col("tmp.y_end"), \
                           F.col("tmp.para_position"))
PARA_WITH_METADATA_DF.show(1, False)
PARA_WITH_METADATA_DF = PARA_WITH_METADATA_DF.withColumn("x", F.regexp_extract(F.col("para_style"), LEFT_REGEX, 3)) \
         .withColumn("y", F.regexp_extract(F.col("para_style"),  TOP_REGEX, 3))

PARA_WITH_METADATA_DF.filter("para_list is not null").count()
PARA_WITH_METADATA_DF.show(1, False)
PARA_WITH_METADATA_DF.filter("para_list like '62%'").show(1, False)

# #############################
# Extracting Lines from Images
# #############################
images_rdd = spark.sparkContext.binaryFiles(INPUT_IMAGES)
line_coord_df = images_rdd.map(lambda img: extract_line_coords(img)).toDF()
line_coord_exp_df = line_coord_df.select("_1", F.explode("_2"))
line_coord_exp_df = line_coord_df.select(F.col("_1").alias("filename"), F.explode(F.col("_2")).alias("coord"))
line_coord_exp_df.show(20, False)
