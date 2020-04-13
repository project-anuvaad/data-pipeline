import happybase
import hashlib
connection = happybase.Connection('sandbox-hdp.hortonworks.com')
connection.open()
table = connection.table('translated_sentences')


def store_translation(source_sentence, target_sentence, source_language, target_language):
    
    print('store_translation for: '+ source_language + '-' + target_language )
    try:
        source_language="en"
        source_sentence = source_sentence
        target_sentence = target_sentence
        target_language = "hi"

        encoded_str = hashlib.sha256(source_sentence.encode())
        hash_hex = encoded_str.hexdigest()
        print(hash_hex)
        row_key=target_language+"_"+hash_hex
        print(row_key) 
        table.put(row_key, {"info:source_sentence": source_sentence})
        table.put(row_key, {"info:target_sentence": target_sentence})
        table.put(row_key, {"info:source_language": "en"})
        table.put(row_key, {"info:target_language": target_language})        

    except Exception as e:
        print('store_translation : Error occurred in storing the translation '', error is == ' + str(e))


def get_translation(source_sentence, source_language, target_language):

    print('get_translation for: '+source_language + '-' + target_language )
    try:
        source_language="en"
        source_sentence = source_sentence
        target_language = "hi"

        encoded_str = hashlib.sha256(source_sentence.encode())
        hash_hex = encoded_str.hexdigest()
        print(hash_hex)
        row_key=target_language+"_"+hash_hex
        print(row_key) 
        row = table.row(row_key)      
        translated_sentence = row[b'info:target_sentence']
        return translated_sentence
    except Exception as e:
        print('store_translation : Error occurred in getting the translation '', error is == ' + str(e))




