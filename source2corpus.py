#! /usr/bin/env python

import sys; sys.path.append('./lib')
from battelleCorpus import BattelleCorpus as BC
    
src_db = 'sources/battelle-sources.db'
dst_db = 'corpus/battelle-corpus.db'

c = BC(src_db,dst_db,nltk_data_path='/lv1/nltk_data')
c.produce()
print('Updating word_freqs in WORD')
c.update_word_freqs()
print('Updating word_stems in WORD')
c.update_word_stems()
#print('Pulling corpus as words')
#c.pull_corpus_as_words()
print('Inserting bigrams')
c.insert_bigrams()
print('Generating mallet corpus')
c.generate_mallet_corpus('models/input/mallet-corpus.csv')