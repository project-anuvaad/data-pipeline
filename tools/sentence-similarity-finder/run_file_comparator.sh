# python3 pdf_sentence_tokenizer.py --input /opt/share/corpus-files/pdfs/5889dda8-f76d-4e81-9136-cd5e137f310e/16613_2016_Judgement_16-Apr-2019.pdf --output /home/anuvaad/aravinth-test/sentence_comparison/sc_tokenized_sentences/sent.out

in_pdf_dir_loc='/home/anuvaad/aravinth-test/sentence_comparison/SC_JUDGEMENTS/SC_JUDGMENTS_2010_2020/2020'
out_txt_dir_loc='/home/anuvaad/aravinth-test/sentence_comparison/sc_tokenized_sentences/2020'
for filepath in ${in_pdf_dir_loc}/*.pdf; 
do
    filename=`basename ${filepath}`
    cmd="python3 pdf_sentence_tokenizer.py --input ${in_pdf_dir_loc}/${filename} --output ${out_txt_dir_loc}/${filename}.txt";
    echo "Starting sentence tokenization. Executing following cmd : "
    echo ${cmd}
    eval ${cmd}
    echo "Completed the tokenization for the file : " ${filename}
    wc -l ${out_txt_dir_loc}/${filename}.txt
    echo "--------------------------------------------------------"
done
echo "Completed tokenizing all the PDFs under the given directory!"
echo "============================================================"
