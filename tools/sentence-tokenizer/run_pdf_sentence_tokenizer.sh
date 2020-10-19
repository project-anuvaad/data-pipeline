# python3 pdf_sentence_tokenizer.py --input /opt/share/corpus-files/pdfs/5889dda8-f76d-4e81-9136-cd5e137f310e/16613_2016_Judgement_16-Apr-2019.pdf --output /home/anuvaad/aravinth-test/sentence_comparison/sc_tokenized_sentences/sent.out

in_pdf_dir_loc='/Users/TIMAC044/Documents/HC_Data/Processing_Input/en'
out_txt_dir_loc='/Users/TIMAC044/Documents/HC_Data/Processing_Output/en'
locale='en'
for filepath in ${in_pdf_dir_loc}/*.pdf; 
do
    filename=`basename ${filepath}`
    #cmd="python3 pdf_sentence_tokenizer.py --input ${in_pdf_dir_loc}/${filename} --output ${out_txt_dir_loc}/${filename}.txt";
    cmd="python3 pdf_to_para_wfm.py --input ${in_pdf_dir_loc}/${filename} --output ${out_txt_dir_loc}/${filename}.txt --locale ${locale}"
		echo "Starting sentence tokenization. Executing following cmd : "
    echo ${cmd}
    eval ${cmd}
    echo "Completed the tokenization for the file : " ${filename}
    wc -l ${out_txt_dir_loc}/${filename}.txt
    echo "--------------------------------------------------------"
done
echo "Completed tokenizing all the PDFs under the given directory!"
echo "============================================================"
