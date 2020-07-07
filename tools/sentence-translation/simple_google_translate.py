# -*- coding: utf-8 -*-

######################################################
# PROJECT : Sample Google Translation
# AUTHOR  : Tarento Technologies
# DATE    : Jul 05, 2020
######################################################

import requests

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ya29.c.Ko8B0gcknmEzX545_8ruNEh0TRzFXayqxyRH-jlpjgBflZniz7Fmq3oPDrAqMfT9iPFAdhqIOoJ8B2Zlz7WgYtBdbwXMjQFhPekmfk9mxkXhhWWvdfUi4PnUn4VeR0DmFsNo4qkbpl78kjCET3jQx7-InO0Hjv7bSt5rIG5ZCIiWnWqA_FyrG1bISJR_0XlAo4s',
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
