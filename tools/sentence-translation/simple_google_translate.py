# -*- coding: utf-8 -*-

######################################################
# PROJECT : Sample Google Translation
# AUTHOR  : Tarento Technologies
# DATE    : Jul 05, 2020
######################################################

import requests

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer XXXXXXXXXXXXX',
}


data = """
{
  'q': 'The Great Pyramid of Giza (also known as the Pyramid of Khufu or the
        Pyramid of Cheops) is the oldest and largest of the three pyramids in
        the Giza pyramid complex.',
  'source': 'en',
  'target': 'hi',
  'format': 'text'
}
"""

response = requests.post('https://translation.googleapis.com/language/translate/v2', headers=headers, data=data)

print(response.text)
