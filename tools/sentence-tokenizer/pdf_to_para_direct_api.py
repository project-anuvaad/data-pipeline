# python3 pdf_to_para.py --input /Users/TIMAC044/Documents/HC_Data/Processing_Input/en/allahabad_hc_2001_ST187_en.pdf --output /Users/TIMAC044/Documents/HC_Data/Processing_Output/en/allahabad_hc_2001_ST187_en.pdf.txt --locale en


# -*- coding: utf-8 -*-

######################################################
# PROJECT : PDF Sentence Tokenizer
# AUTHOR  : Tarento Technologies
# DATE    : May 4, 2020
######################################################

import argparse
import json
import os
import requests
import sys


'''
This code is used to extract tokenized sentences from an input pdf.
As a first step, API call is made to get all the paragraphs from the pdf.
Second step is to tokenize the sentences from each of the paragraph.

Sample run : 
python pdf_sentence_tokenizer.py -i /path/to/file/location/ex3_16613_2016_Judgement_16-Apr-2019.pdf -o /path/to/out/location/out.txt
'''

msg = "Adding description"

# Initialize parser & add arguments
parser = argparse.ArgumentParser(description = msg)
parser.add_argument("-i", "--input", help = "Input pdf file")
parser.add_argument("-o", "--output", help = "Output text file")
parser.add_argument("-l", "--locale", help = "language locale")
args = parser.parse_args()

if args.input is None:
    sys.exit("ERROR : input variable missing!")

if args.output is None:
    sys.exit("ERROR : output variable missing!")

if args.locale is None:
    sys.exit("ERROR : language locale missing!")

print("Passed inputs : ")
print("----------------")
print("Input File  : " + args.input)
print("Output File : " + args.output)
print("Lang Locale : " + args.locale)


# Extract para API variables
EXTRACT_PARA_FILE = args.input


upload_url = "https://auth.anuvaad.org/api/v0/upload-file"
mb_url = "https://auth.anuvaad.org/api/v0/merge-blocks-wf"
download_url = "https://auth.anuvaad.org/download/"

lang_locale = args.locale
auth_key = "Enter your token here"


print("\nStarting to post the extract-paragraphs API call...")

try:
	#####################################
    # UPLOAD PDF API
    #####################################
    payload = {}
    files = [
      ('file', open(EXTRACT_PARA_FILE,'rb'))
    ]
    headers = {
      'Authorization': auth_key
    }

    upload_response = requests.request("POST", upload_url, headers=headers, data = payload, files = files)
    uploaded_pdf_name = upload_response.json()["data"]
    # print(uploaded_pdf_name)

    #####################################
    # MERGE BLOCK API
    #####################################
    payload = "{\"input\": {\r\n        \"files\": [\r\n            {\r\n                \"locale\": \"input_locale\",\r\n                \"path\": \"input_path\",\r\n                \"type\": \"pdf\"\r\n            }\r\n        ]},\r\n        \"jobID\": \"BM-15913540488115873\",\r\n        \"state\": \"INITIATED\",\r\n        \"status\": \"STARTED\",\r\n        \"stepOrder\": 0,\r\n        \"workflowCode\": \"abc\",\r\n        \"tool\":\"BM\"\r\n}"
    headers = {
      'Authorization': auth_key,
      'Content-Type': 'text/plain'
    }

    mb_response = requests.request("POST", mb_url, headers=headers, data = payload.replace('input_locale',lang_locale).replace('input_path', uploaded_pdf_name))
    tokenized_para_json_file = mb_response.json()["output"][0]["outputFilePath"]
    #print(tokenized_para_json_file)

    #####################################
    # DOWNLOAD PARAS API
    #####################################
    final_resp_json = requests.request("GET", download_url+tokenized_para_json_file, headers={}, data = {})

    with open(EXTRACT_PARA_FILE +".txt", 'w', encoding = 'utf-8') as f:
      for s_line in final_resp_json.json()['result']:
        for page in s_line['text_blocks']:
           #print(page['text'])
           f.write(page['text'] + '\n')

except Exception as e:
  print("Error in file operation! Try again..")
  sys.exit(e.message)


print("Completed generating the tokenized sentences.")
