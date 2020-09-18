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
args = parser.parse_args()

if args.input is None:
    sys.exit("ERROR : input variable missing!")

if args.output is None:
    sys.exit("ERROR : output variable missing!")

print("Passed inputs : ")
print("----------------")
print("Input File  : " + args.input)
print("Output File : " + args.output)


# Extract para API variables
EXTRACT_PARA_FILE = args.input
# EXTRACT_PARA_FILE ='/opt/share/corpus-files/pdfs/9289b73e-7be1-4a4a-852e-afe0270bc192/23036_2019_2_1501_20227_Judgement_04-Feb-2020.pdf'
EXTRACT_PARA_URL  = "https://auth.anuvaad.org/v1/interactive-editor/extract-paragraphs"
EXTRACT_PARA_CT   = "application/octet-stream"

# Tokenize Sentence variables
TOKENIZE_SENT_URL = "https://auth.anuvaad.org/v2/tokenize-sentence"
TOKENIZE_SENT_HEADERS = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer a8fc0a28d4f447e58cfacabaf5625022|8abc26848d1240568176439af85a5f9d'
}

print("\nStarting to post the extract-paragraphs API call...")

try:
  FILES = {'pdf_data': (os.path.basename(EXTRACT_PARA_FILE), open(EXTRACT_PARA_FILE, 'rb'), EXTRACT_PARA_CT)}
  ep_response = requests.post(url=EXTRACT_PARA_URL, files=FILES)
  print("Post request completed and response received.\n")
  ep_resp_json = json.loads(ep_response.content)
  
  print("Starting the Tokenize Sentences API calls...")
  with open(args.output,'w', encoding = 'utf-8') as f:
    for line in ep_resp_json['data']:
      TOKENIZE_SENT_PAYLOAD = """{
                              "paragraphs": [
                                {
                                  "text": \""""
      TOKENIZE_SENT_PAYLOAD += str(line['text']).replace('"','').replace('“', '').replace('”', '').strip("'<>() ")
      TOKENIZE_SENT_PAYLOAD += """", "page_no":"""
      TOKENIZE_SENT_PAYLOAD += str(line['page_no'])
      TOKENIZE_SENT_PAYLOAD += """
                               }],
                               "lang": "English"}
                               """    
      ts_response = requests.request("POST", TOKENIZE_SENT_URL, headers=TOKENIZE_SENT_HEADERS, data=TOKENIZE_SENT_PAYLOAD.encode('utf-8'))
      ts_resp_json = json.loads(ts_response.content)
      for s_line in ts_resp_json['data']:
        for sentence in s_line['text']:
          f.write(sentence + '\n')

except Exception as e:
  print("Error in file operation! Try again..")
  sys.exit(e.message)


print("Completed generating the tokenized sentences.")
