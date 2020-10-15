# python3 pdf_to_para_wfm.py --input "/Users/TIMAC044/Documents/HC_Data/Allahabad_HC/allahabad_hc_2016_JA2946_hi.pdf" --output "/Users/TIMAC044/Documents/HC_Data/Allahabad_HC/allahabad_hc_2016_JA2946_hi.pdf.txt" --locale "hi"


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
import time


'''
This code is used to extract tokenized sentences from an input pdf.
As a first step, API call is made to get all the paragraphs from the pdf.
Second step is to tokenize the sentences from each of the paragraph.

Sample run : 
python pdf_to_para_wfm.py -i /tmp/ma_hc_2017_WA1248_alt_ta.pdf -o /tmp/out.txt -l ta
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


input_pdf_file = args.input
lang_locale    = args.locale
output_file    = args.output
auth_key = "Enter your token here"

# DEFINE API endpoints
base_url       = "https://auth.anuvaad.org/" 
upload_url     = base_url + "api/v0/upload-file"
mb_url         = base_url + "api/v0/merge-blocks-wf"
wfm_url        = base_url + "anuvaad-etl/wf-manager/v1/workflow/initiate"
job_status_url = base_url + "anuvaad-etl/wf-manager/v1/workflow/jobs/search/bulk"
download_url   = base_url + "download/"


# FUNCTIONS
def get_tokenization_result(job_id):
    payload = "{\r\n    \"jobIDs\": [\"input_jobid\"],\r\n    \"taskDetails\":true\r\n}"
    headers = {
      'Authorization': auth_key,
      'Content-Type': 'application/json'
    }
    try:
        r = requests.request("POST", job_status_url, headers=headers, data = payload.replace('input_jobid',input_jobid))
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print (e.response.text)
        return None, None
    rsp = r.json()
    # print(rsp)
    if rsp is not None and len(rsp) > 0:
        if rsp[0]['status'] == 'COMPLETED':
            return True, rsp
    return False, rsp


#####################################
# UPLOAD PDF API
#####################################

payload = {}
files = [
  ('file', open(input_pdf_file,'rb'))
]
headers = {
  'Authorization': auth_key
}

upload_response = requests.request("POST", upload_url, headers=headers, data = payload, files = files)

uploaded_pdf_name = upload_response.json()["data"]
# print(uploaded_pdf_name)


#####################################
# INITIATE BM WFM
#####################################
payload = "{\r\n\"files\": [ {\r\n\"locale\": \"input_locale\",\r\n\"path\": \"input_path\",\r\n\"type\": \"pdf\"\r\n}\r\n],\r\n\"workflowCode\": \"DP_WFLOW_FBT\"\r\n}"
headers = {
  'Authorization': auth_key,
  'Content-Type': 'application/json'
}

response = requests.request("POST", wfm_url, headers=headers, data = payload.replace('input_locale',lang_locale).replace('input_path', uploaded_pdf_name))
input_jobid = response.json()['jobID']
# print(input_jobid)


#####################################
# CHECK WFM JOB STATUS
#####################################
prev_status = ''
output_json = ''
while(1):
    status, rsp = get_tokenization_result(input_jobid)
    if(rsp[0]['status'] != prev_status):
        prev_status = rsp[0]['status']
        print(prev_status)
    if status:
        print('jobId %s, completed successfully' % input_jobid)
        output_json = rsp[0]['output'][0]['outputFile']
        break
    else:
        print('jobId %s, still running, waiting for 10 seconds' % input_jobid)
        time.sleep(30)
# print(output_json)

#####################################
# DOWNLOAD PARAS API
#####################################
final_resp_json = requests.request("GET", download_url + output_json, headers={}, data = {})

with open(output_file, 'w', encoding = 'utf-8') as f:
  for s_line in final_resp_json.json()['result']:
    for block in s_line['text_blocks']:
      for sentences in block['tokenized_sentences']:
        if(sentences is not None):
            f.write(sentences['src'] + '\n')



print("Completed generating the tokenized sentences.")
