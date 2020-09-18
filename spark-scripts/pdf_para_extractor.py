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
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, BooleanType, MapType, StringType, StructType, StructField, IntegerType




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

CONVERTED_OUT_DIR = "/Users/TIMAC044/Documents/Anuvaad/converted_htmls/ex1/*"
 # Util class for identifying a rectangle
RECT_UTIL    = '/Users/TIMAC044/Documents/Anuvaad/table-detection/rect.py'
# Util class for identifying a rectangle
TABLE_UTIL   = '/Users/TIMAC044/Documents/Anuvaad/table-detection/table.py'

INPUT_HTML_DOCS   = CONVERTED_OUT_DIR + "--25.html"
INPUT_IMAGES      = CONVERTED_OUT_DIR + "*.png"
NUM_PARTITIONS    = 10
# Define Regex
END_OF_SENT_REGEX = """(([\"|”|,|a-zA-Z|0-9|.]{3,}[.|?|!|\"|”|:|;]|([:][ ][-]))$)"""
STYLE_REGEX       = """.(.*){(.*)font-size:([0-9]*)px;(.*)font-family:(.*);(.*)color:(.*);(.*)"""
TOP_REGEX         = """(.*)(top:)([0-9]*)(.*)"""
LEFT_REGEX        = """(.*)(left:)([0-9]*)(.*)"""
PAGE_NUM_POSSIBLE_FRMT = """(Page|page)(\s)([0-9]*)(\s)(of)(\s)([0-9]*)"""
ABBRIVATIONS2 = [' no.', ' mr.', ' ft.', ' kg.', ' dr.', ' ms.', ' st.', ' pp.', ' co.', ' rs.', ' sh.', ' vs.', ' ex.']
ABBRIVATIONS3 = [' pvt.', ' nos.', ' smt.', ' sec.', ' spl.', ' kgs.', ' ltd.', ' pty.', ' vol.', ' pty.', ' m/s.', ' mrs.', ' i.e.', ' etc.', ' (ex.', ' o.s.', ' anr.', ' ors.', ' c.a.']
ABBRIVATIONS4 = [' assn.']
ABBRIVATIONS6 = [' w.e.f.']

# Add Util classes to the Spark Context
spark.sparkContext.addPyFile(RECT_UTIL)
spark.sparkContext.addPyFile(TABLE_UTIL)

# Define sql Context
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)


# Define String to Array UDF
def parse_array_from_string(x):
    return ast.literal_eval(x)

RETRIEVE_ARRAY_UDF = F.udf(parse_array_from_string, T.ArrayType(T.StringType()))



# Define HTML Parsing UDF
def parse_html_tags(x):
    page_no_list = []
    content_list = []
    para = ''
    soup = BeautifulSoup (x, 'html.parser')
    # Extact the numeric value alone
    page_no_list.append(str(soup.title.string).split()[1])
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
        clean_line = str(line).replace('<br/>','') \
                              .replace('<i>','') \
                              .replace('<i/>','') \
                              .replace('</i>','') \
                              .replace('<b>','') \
                              .replace('</b>','') \
                              .replace('<b/>','')
        # Make the sentence single-spaced and remove leading/trailing spaces
        clean_line = ' '.join(clean_line.split()).strip()
        # Extract the required sections (class, mapped style, p style, actual text)
        para_reg = re.search('<p class="(.*)" style=\"(.*)\">(.*)</p>', clean_line)
        formatted_text += ['{0}\t{1}\t{2}\t{3}'.format(para_reg.group(1), style_map.get(para_reg.group(1)), para_reg.group(2), para_reg.group(3))]
    content_list.append(str(formatted_text))
    return page_no_list, content_list


parse_html_tags_schema = T.StructType([
    T.StructField('page_no', T.ArrayType(T.StringType()), False),
    T.StructField('contents', T.ArrayType(T.StringType()), False),
])

PARSE_HTML_TAGS_UDF = F.udf(parse_html_tags, parse_html_tags_schema)



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
APPEND_DF = HTML_DF.withColumn("metadata", PARSE_HTML_TAGS_UDF(HTML_DF.html_page_tags)) \
                   .select(HTML_DF.filepath, F.col('metadata.*'))

EXPLODED_DF = APPEND_DF.withColumn("tmp", F.arrays_zip("page_no", "contents")) \
                   .withColumn("tmp", F.explode("tmp")) \
                   .select(APPEND_DF.filepath, F.col("tmp.page_no"), F.col("tmp.contents"))

# UDF to extract the filename from the File path
FILENAME_EXT_UDF = F.udf(lambda f : re.match(r'(.*/)(.*)(--[0-9]*)(\.html)', f).group(2), StringType())
TRIM_FN_DF = EXPLODED_DF.select(FILENAME_EXT_UDF(EXPLODED_DF.filepath).alias("filename"), EXPLODED_DF.page_no, EXPLODED_DF.contents)


# Section identifies the style of the paragraph (constant across the entire doc)
EXPLODED_SENT_DF  = TRIM_FN_DF.select("filename", "page_no", F.explode(RETRIEVE_ARRAY_UDF(F.col("contents"))))
split_col         = F.split(EXPLODED_SENT_DF['col'], '\t')
EXPLODED_SENT_DF  = EXPLODED_SENT_DF.withColumn("style_key", split_col.getItem(1))
STYLE_COUNT_DF    = EXPLODED_SENT_DF.groupBy("filename", "style_key").count()

# Identify the para style for each of the PDFs.
window            = Window.partitionBy("filename").orderBy(F.desc("count"))
MAX_OCCURANCE_DF  = STYLE_COUNT_DF.withColumn("rank", F.rank().over(window)).filter('rank==1') \
                                  .select("filename", F.col("style_key").alias("para_style_key"))

#para_style        = MAX_OCCURANCE_DF.select('filename', 'style_key').first().style_key


LINES_WITH_STYLEID_DF = TRIM_FN_DF.join(MAX_OCCURANCE_DF, TRIM_FN_DF.filename == MAX_OCCURANCE_DF.filename) \
                                  .select(TRIM_FN_DF.filename, TRIM_FN_DF.page_no, TRIM_FN_DF.contents, MAX_OCCURANCE_DF.para_style_key)
#LINES_WITH_STYLEID_DF.show()



# ######################################
# Extracting Footer lines from Images
# ######################################
IMG_FILENAME_UTILS_UDF = F.udf(lambda f,n : re.match(r'(.*)(-)(0*)(.*)(\.png)', f).group(n), StringType())
IMAGES_RDD = spark.sparkContext.binaryFiles(INPUT_IMAGES)

LINE_COORD_DF = IMAGES_RDD.map(lambda img: extract_line_coords(img)).toDF()
#LINE_COORD_EXP_DF = LINE_COORD_DF.select("_1", F.explode("_2"))
LINE_COORD_EXP_DF = LINE_COORD_DF.select(F.col("_1").alias("filename"), \
                                         F.explode(F.col("_2")).alias("coord"))
LINE_COORD_EXP_DF = LINE_COORD_EXP_DF.select(F.col("filename"), \
                                         IMG_FILENAME_UTILS_UDF(F.col("filename"), F.lit(1)).alias("file_id"), \
                                         IMG_FILENAME_UTILS_UDF(F.col("filename"), F.lit(4)).alias("page_num"), \
                                         F.col("coord._1").alias("x"), \
                                         F.col("coord._2").alias("y"), \
                                         F.col("coord._3").alias("w"), \
                                         F.col("coord._4").alias("h"))
LINE_COORD_EXP_DF.createOrReplaceTempView('line_coord_exp_df')
# Condition for Footer line.
footer_list = spark.sql("""
                SELECT 
                  file_id, page_num, x, w, 
                  COUNT(*) cnt,
                  MIN(y) min_y,
                  MAX(y) max_y
                FROM line_coord_exp_df 
                GROUP BY file_id, page_num, x, w 
                HAVING (min_y > 1000 and (cnt==1 OR max_y-min_y>150)) 
                ORDER BY file_id ASC, cast(page_num AS int) ASC
            """).collect()
ftr = spark.sparkContext.broadcast(footer_list)

footer_coord_lookup = dict()
for f in footer_list:
    #print("{0}\t{1}\t{2}".format(f.file_id, f.page_num, f.min_y))
    footer_coord_lookup[f.file_id + "#" + str(f.page_num)] = f.min_y


# ######################################
# Extracting Tables from Images
# ######################################

def extract_table_coords(image):
  from rect import RectRepositories
  from table import TableRepositories
  name, img       = image
  pil_image       = Image.open(io.BytesIO(img)).convert('RGB') 
  open_cv_image   = numpy.array(pil_image) 
  open_cv_image  = open_cv_image[:, :, ::-1].copy() 
  Rects           = RectRepositories(open_cv_image)
  lines, _        = Rects.get_tables_and_lines ()
  table           = None
  TableRepo       = TableRepositories(open_cv_image, table)
  tables          = TableRepo.response ['response'] ['tables']
  lines           = []
  for table in tables:
    base_x = int(table.get('x'))
    base_y = int(table.get('y'))
    for t in table.get('rect'):
      x = base_x + int(t['x'])
      y = base_y + int(t['y'])
      w = int(t['w'])
      h = int(t['h'])
      row = int(t['row'])
      col = int(t['col'])
      lines.append((row, col, x, y, w, h))
  return os.path.basename(name), lines

# For each of the images, extract the coordinates along with row & column
TABLE_COORD_DF       = IMAGES_RDD.map(lambda img: extract_table_coords(img)).toDF()

# Explode the cells (one row per cell)
TABLE_COORD_EXP_DF   = TABLE_COORD_DF.select(F.col("_1").alias("filename"), \
                                         F.explode(F.col("_2")).alias("coord"))
# Final view
FINAL_TABLE_COORD_DF = TABLE_COORD_EXP_DF.select("filename", \
                                    IMG_FILENAME_UTILS_UDF(F.col("filename"), F.lit(1)).alias("file_id"), \
                                    IMG_FILENAME_UTILS_UDF(F.col("filename"), F.lit(4)).alias("page_num"), \
                                    F.col("coord._1").alias("row"), \
                                    F.col("coord._2").alias("col"), \
                                    F.col("coord._3").alias("x"), \
                                    F.col("coord._4").alias("y"), \
                                    F.col("coord._5").alias("w"), \
                                    F.col("coord._6").alias("h"))

# FINAL_TABLE_COORD_DF.show(500, False)
FINAL_TABLE_COORD_DF.createOrReplaceTempView('final_table_coord_df')

table_list = spark.sql("""
                SELECT 
                  file_id, page_num, row, col, x, y, w, h
                FROM 
                  final_table_coord_df 
                ORDER BY 
                  file_id ASC, cast(page_num AS int) ASC, y ASC
            """).collect()
tlist = spark.sparkContext.broadcast(table_list)

table_coord_lookup = dict()
for f in table_list:
    #print("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}".format(f.file_id, f.page_num, f.row, f.col, f.x, f.y, f.w, f.h))
    cell_record = dict()
    cell_record['row'] = f.row
    cell_record['col'] = f.col
    cell_record['x']   = f.x
    cell_record['y']   = f.y
    cell_record['w']   = f.w
    cell_record['h']   = f.h
    lookup_key = f.file_id + "#" + str(f.page_num)
    # if key is already available, append the cell record to the list.
    if lookup_key in table_coord_lookup:
        existing_list = table_coord_lookup[lookup_key]
        existing_list.append(cell_record)
    # else create a new list with the cell record and insert into the lookup.
    else:
        cell_list = list()
        cell_list.append(cell_record)
        table_coord_lookup[lookup_key] = cell_list


# content is the set of lines containing class, style_key, line style & line text
# doc_p_style is the derived paragraph style for the entire document.
def parse_para(content, file_id, page_no, doc_p_style):
    out_para_list  = []
    # Pick the first style for the entire para
    out_style_list = []
    out_page_num_list  = []
    out_y_end_list = []
    out_para_position_list = []
    out_sup_list  = []
    lines = parse_array_from_string(content)
    para_text    = ''
    para_style   = ''
    indexed_prev_top_val = -1
    curr_top_val = ''
    processed_index = 0
    l_class_val_list  = []
    l_style_key_list  = []
    l_p_style_list    = []
    l_p_text_list     = []
    l_top_list        = []
    l_left_list       = []
    len_page          = len(lines)
    f_min_y           = "-1"
    table_coords      = list()
    img_page_key      = file_id + "#" + str(page_no)
    is_table          = False
    # Header
    if img_page_key in footer_coord_lookup.keys():
        f_min_y = footer_coord_lookup[img_page_key]
    # Table
    if img_page_key in table_coord_lookup.keys():
        table_coords = table_coord_lookup[img_page_key]
    for line in lines:
        parts        = line.split('\t')
        l_class_val_list.append(parts[0].strip())
        l_style_key_list.append(parts[1].strip())
        l_p_style_list.append(parts[2].strip())
        l_p_text_list.append(parts[3].strip())
        # Extract Top Value
        style_values = re.search(TOP_REGEX, parts[2], re.IGNORECASE)
        if style_values:
            l_top_list.append(style_values.group(3))
        else:
            l_top_list.append("-1")
        # Extract Left Value
        style_values = re.search(LEFT_REGEX, parts[2], re.IGNORECASE)
        if style_values:
            l_left_list.append(style_values.group(3))
        else:
            l_left_list.append("-1")
    # Logic to find last para line of the page.
    last_line_index = len_page - 1
    for lstyle in reversed(l_style_key_list):
        if(lstyle == doc_p_style and l_p_text_list[last_line_index].strip()!=''):
            break
        last_line_index -= 1
    for index in range(len_page):
        l_class_val = l_class_val_list[index]
        l_style_key = l_style_key_list[index]
        l_p_style   = l_p_style_list[index]
        l_next_p_style   = l_style_key_list[index+1] if (index<len_page-1) else "-1"
        l_p_text    = l_p_text_list[index]
        sentence_end = re.search(END_OF_SENT_REGEX, l_p_text, re.IGNORECASE) and not \
                            (l_p_text[-4:] in ABBRIVATIONS2 \
                            or l_p_text[-5:] in ABBRIVATIONS3 \
                            or l_p_text[-6:] in ABBRIVATIONS4 \
                            or l_p_text[-7:] in ABBRIVATIONS6)
        if(para_text == ''):
            para_style = l_p_style
        curr_top_val = int(l_top_list[index])
        prev_top_val = int(l_top_list[index-1])
        next_top_val = int(l_top_list[index+1]) if (index<len_page-1) else -1

        # Code added for table
        text_contained_within_table = False
        curr_left_val = int(l_left_list[index])
        prev_text_available_cell = dict()
        text_available_cell = dict()
        for coord in table_coords:
          if (curr_top_val >= coord['y'] and curr_top_val <= (coord['y'] + coord['h'])):
            text_contained_within_table = True
            text_available_cell = coord
            break

        l_para_pos = "regular" 
        if (text_contained_within_table):
            l_para_pos = "table"
        if (curr_top_val < 75):
            l_para_pos = "header"
        elif (processed_index == 0):
            l_para_pos = "start"
        elif (index == last_line_index):
            l_para_pos = "end_complete" if (sentence_end) else "end_incomplete"
        elif (curr_top_val > 1175 or (int(f_min_y) > 0 and curr_top_val > int(f_min_y))):
            l_para_pos = "footer"

        # Case 1 : Ignore Header (including Page No.)
        if ((index <= 3 or (len_page-index) <= 3) and (curr_top_val < 75 or curr_top_val > 1175)):
            # Page number Format
            if(re.match(PAGE_NUM_POSSIBLE_FRMT, l_p_text) or l_p_text.strip() == page_no or l_p_text.strip() == ''):
                continue
            else:
                para_text += l_p_text
            
            out_para_list  += [para_text]
            out_style_list += [para_style]
            out_y_end_list += [curr_top_val]
            out_para_position_list.append(l_para_pos)
            para_text = ''
        # Case 2 : Handle Footer
        elif (int(f_min_y) > 0 and curr_top_val > int(f_min_y)):
            if (index == len_page - 1):
              # Ignore the page number
              if(l_p_text.strip() != page_no):
                para_text += l_p_text
              
              out_para_list  += [para_text]
              out_style_list += [para_style]
              out_y_end_list += [curr_top_val]
              out_para_position_list.append(l_para_pos)
            else:
              para_text += l_p_text
              para_text += ' '
        # Case : Table contents
        elif (len(table_coords) > 0 and text_contained_within_table):
            # Cond 1 : When the table is starting, flush all the contents before it
            # Cond 2 : When content is not continuation within the same cell, flush it
            if (not is_table or not (text_available_cell['x']==prev_text_available_cell['x'] and text_available_cell['y']==prev_text_available_cell['y'])):
                out_para_list  += [para_text]
                out_style_list += [para_style]
                out_y_end_list += [curr_top_val]
                out_para_position_list.append(l_para_pos)
                # Reset
                is_table = True
                para_text = ''
                prev_text_available_cell = text_available_cell
            para_text += l_p_text
            para_text += ' '
            print("Table content : " + str(curr_top_val) + " :: " + str(coord['y']) + " :: " + str(coord['h']) + " :: " + para_text)
        # Case 3 : Non-para Style Condition
        elif (l_style_key != doc_p_style):
            # Condition : Previous line was end of table
            if (is_table):
                out_para_list  += [para_text]
                out_style_list += [para_style]
                out_y_end_list += [curr_top_val]
                out_para_position_list.append(l_para_pos)
                is_table = False
                para_text = ''
            if (para_text == ''):
                para_text += l_p_text
            elif (indexed_prev_top_val == curr_top_val \
                   or sentence_end \
                   or l_style_key == l_style_key_list[index-1] \
                   or abs(prev_top_val - next_top_val) < 5):
                para_text += l_p_text
                para_text += ' '
            

            # Handling superscripts - appearing in the middle of the line
            if (para_text.strip()==''):
                continue
            # Case : super script
            elif (index<len_page-1 and prev_top_val==next_top_val):
                out_sup_list.append(l_p_text)
            # Case : Sentence ends, but paragraph continues
            elif (sentence_end and l_style_key == l_next_p_style and l_para_pos != 'end_incomplete' ):
                continue
            # Case for writing the output
            elif (l_para_pos == 'end_incomplete' or sentence_end or l_next_p_style == doc_p_style):
                out_para_list  += [para_text]
                out_style_list += [para_style]
                out_y_end_list += [curr_top_val]
                out_para_position_list.append(l_para_pos)
                processed_index+= 1
                para_text = ''
            indexed_prev_top_val = -1
        # Case 4 : Para Style Condition
        else:
            # Condition : Previous line was end of table
            if (is_table):
                out_para_list  += [para_text]
                out_style_list += [para_style]
                out_y_end_list += [curr_top_val]
                out_para_position_list.append(l_para_pos)
                is_table = False
                para_text = ''
            para_text += l_p_text
            # Case : Sentence ends or reaches end of page
            if(sentence_end or l_para_pos == 'end_incomplete'):
                if(para_text.strip() != ''):
                    out_para_list  += [para_text]
                    out_style_list += [para_style]
                    out_y_end_list += [curr_top_val]
                    out_para_position_list.append(l_para_pos)
                    processed_index += 1
                para_text = ''
            else:
                para_text += ' '
            indexed_prev_top_val = curr_top_val
    # Flush remaining para
    if(para_text.strip() != ''):
        out_para_list  += [para_text]
        out_style_list += [para_style]
        out_y_end_list += [curr_top_val]
        out_para_position_list.append("end_incomplete")
    return out_para_list, out_style_list, out_page_num_list, out_y_end_list, out_para_position_list


parse_para_schema = T.StructType([
    T.StructField('para_list', T.ArrayType(T.StringType()), False),
    T.StructField('para_style', T.ArrayType(T.StringType()), False),
    T.StructField('page_num', T.ArrayType(T.StringType()), False),
    T.StructField('y_end', T.ArrayType(T.StringType()), False),
    T.StructField('para_position', T.ArrayType(T.StringType()), False),
])

PARSE_PARA_UDF = F.udf(parse_para, parse_para_schema)

AGG_PARA_DF = LINES_WITH_STYLEID_DF.select(F.col("filename"), \
                                           F.col("page_no"), \
                                           (PARSE_PARA_UDF(F.col("contents"), F.col("filename"), F.col("page_no"), F.col("para_style_key"))).alias('metrics')) \
                                   .select("filename","page_no", F.col('metrics.*'))


PARA_WITH_METADATA_DF = AGG_PARA_DF.withColumn("tmp", F.arrays_zip("para_list", "para_style", "page_num", "y_end", "para_position")) \
                   .withColumn("tmp", F.explode("tmp")) \
                   .select(F.col("filename"), \
                           F.col("page_no"), \
                           F.col("tmp.para_list"), \
                           F.col("tmp.para_style"), \
                           F.col("tmp.page_num"), \
                           F.col("tmp.y_end"), \
                           F.col("tmp.para_position"))

PARA_WITH_METADATA_DF = PARA_WITH_METADATA_DF.withColumn("x", F.regexp_extract(F.col("para_style"), LEFT_REGEX, 3)) \
         .withColumn("y", F.regexp_extract(F.col("para_style"),  TOP_REGEX, 3))

# Handle the sentences that are spanning across pages.
FIRST_LAST_PARA_DF = PARA_WITH_METADATA_DF.filter('para_position=="start" or para_position=="end_incomplete"')
P_DF1_ALIAS = FIRST_LAST_PARA_DF.alias('df1')
P_DF2_ALIAS = FIRST_LAST_PARA_DF.alias('df2')

INTERSECTION_SENT_DF = P_DF1_ALIAS.join(P_DF2_ALIAS, F.col("df1.page_no").cast(IntegerType()) == (F.col("df2.page_no").cast(IntegerType()) + 1)) \
                        .select(F.col("df1.filename").alias("f1_filename"), 
                                F.col("df1.page_no").alias("f1_page_no"), 
                                F.col("df1.para_list").alias("f1_para_list"), 
                                F.col("df1.y_end").alias("f1_y_end"), 
                                F.col("df1.para_position").alias("f1_para_position"),
                                F.col("df1.para_style").alias("f1_para_style"),
                                F.col("df1.x").alias("f1_x"),
                                F.col("df1.y").alias("f1_y"),
                                F.col("df2.filename").alias("f2_filename"), 
                                F.col("df2.page_no").alias("f2_page_no"), 
                                F.col("df2.para_list").alias("f2_para_list"), 
                                F.col("df2.y_end").alias("f2_y_end"), 
                                F.col("df2.para_position").alias("f2_para_position"), 
                                F.col("df2.para_style").alias("f2_para_style"),
                                F.col("df2.x").alias("f2_x"),
                                F.col("df2.y").alias("f2_y")) \
                        .filter('f1_filename == f2_filename and f1_para_position == "start" and f2_para_position == "end_incomplete"') \
                        .withColumn('complete_para', F.concat(F.col('f2_para_list'), F.lit(' '), F.col('f1_para_list')))


# Regular para - contained within same page.
REG_PARA_DF   = PARA_WITH_METADATA_DF.filter('para_position NOT IN ("start","end_incomplete")')

# Split para - contained across multiple pages.
SPLIT_PARA_DF = INTERSECTION_SENT_DF.select(
                                        F.col("f2_filename").alias("filename"),
                                        F.col("f2_page_no").alias("page_no"),
                                        F.col("complete_para").alias("para_list"),
                                        F.col("f2_para_style").alias("para_style"),
                                        F.col("f2_page_no").alias("page_num"),
                                        F.col("f1_y_end").alias("y_end"),
                                        F.col("f2_para_position").alias("para_position"),
                                        F.col("f1_x").alias("x"),
                                        F.col("f1_y").alias("y")
                                    )

# Aggregate the regular para & the split para.
UNION_PARA_DF = REG_PARA_DF.union(SPLIT_PARA_DF)

# UNION_PARA_DF.filter("filename='ex2'").select("page_no", "para_position", "para_list").orderBy(["page_no"], ascending=True).show(500, False)

# Output in required format
UNION_PARA_DF.filter("para_list IS NOT NULL and filename=='out.html'").select("para_list").coalesce(1).write \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("/tmp/output")








