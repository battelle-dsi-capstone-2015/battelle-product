import re, nltk
from gensim.corpora import Dictionary
from database import Database

class Corpus:
    
    NOALPHA = re.compile('\W+')
    MSPACES = re.compile('\s+')

    # This is passed to a Database object in the constructor, but is put
    # here to document the model for static inspection
    db_schema = {
        'doc': ('doc_index INTEGER','doc_id INTEGER PRIMARY KEY','doc_label TEXT','doc_str TEXT'),
        'word': ('word_id INTEGER PRIMARY KEY','word_str TEXT','word_freq INTEGER','word_stem TEXT'),
        'docword': ('doc_index INTEGER','doc_id INTEGER', 'word_id INTEGER', 'word_str TEXT', 'word_count INTEGER'),
        'bigram': ('word_str_1 TEXT','word_str_2 TEXT','bigram_score REAL')
    }

    def __init__(self,dbfile,dictfile=None,nltk_data_path=None):
        self.dictfile = dictfile
        self.dbi = Database(dbfile,self.db_schema)
        if nltk_data_path:
            nltk.data.path.append(nltk_data_path)

    # This method is abstract and must be overwritten for each case.
    # The generator should yield a three item list or tuple
    # (doc_id,doc_label,doc_content). The generator should also make
    # sure doc_content is unique. In addition, here is where you can
    # apply other preprocess filers such as lemmatization.
    def src_doc_generator(self):
        pass

    # One problem with Gensim is that it does not preserve document
    # IDs and tags; it relies on the list index. So at some point you
    # need to create a mapping between the source doc_id and the local
    # gensim id. This will require changing the generator so that it
    # supplies these values in addition to the text string. Also, need
    # to make stopwords configurable. And leave NLP preprocessing to
    # the generator.
    def produce(self):        

        print('Getting src docs')
        docs = []
        doctokens = [] # aka Gensim's "text"
        stopwords = nltk.corpus.stopwords.words('english')
        for doc in self.src_doc_generator():
            (doc_id,doc_label,doc_str) = doc
            docs.append(doc)
            doctokens.append([token for token in nltk.word_tokenize(doc_str) if token not in stopwords])
            if len(docs) % 1000 == 0: print(len(docs))
                
        print('Creating the dictionary')
        dictionary = Dictionary(doctokens)
        #dictionary.compactify()
        #dictionary.filter_extremes(keep_n=None)
        if self.dictfile:
            dictionary.save_as_text(self.dictfile+'.dict', sort_by_word=True)

        with self.dbi as db:

            print('Creating WORD') # aka Gensim's "dictionary"
            db.create_table('word')
            for word_id, word_str in dictionary.iteritems():
                db.cur.execute('INSERT INTO word (word_id, word_str) VALUES (?,?)',(word_id,word_str))
            
            print('Creating DOC and DOCWORD')
            db.create_table('doc')
            db.create_table('docword')
            for doc_idx, doc in enumerate(docs):
                db.cur.execute('INSERT INTO doc (doc_index,doc_id,doc_label,doc_str ) VALUES (?,?,?,?)',(doc_idx,doc[0],doc[1],doc[2]))
                doc_id = doc[0]
                for word_id, word_count in (dictionary.doc2bow(doctokens[doc_idx])):
                    word_str = dictionary.get(word_id) # Is this valid? I believe it is.
                    db.cur.execute('INSERT INTO docword (doc_index,doc_id,word_id,word_str,word_count) VALUES (?,?,?,?,?)',(doc_idx,doc_id,word_id,word_str,word_count))

    def update_word_freqs(self):
        with self.dbi as db:
            rows = []
            for r in db.cur.execute("SELECT sum(word_count) as 'word_freq', word_id FROM docword GROUP BY word_id"):
                rows.append(r)
            db.cur.executemany("UPDATE word SET word_freq = ? WHERE word_id = ?",rows)

    def update_word_stems(self):
        from nltk.stem import PorterStemmer
        st = PorterStemmer()
        with self.dbi as db:
            rows = []
            for r in db.cur.execute('SELECT word_id, word_str FROM word'):
                stem = st.stem(r[1])
                rows.append((stem,r[0]))
            db.cur.executemany("UPDATE word SET word_stem = ? WHERE word_id = ?",rows)

    def pull_corpus_as_words(self):
        self.doc_words = []
        with self.dbi as db:
            for r in db.cur.execute("SELECT doc_str FROM doc ORDER BY doc_index"):
                for word in r[0].split():
                    self.doc_words.append(word)
    
    def insert_bigrams(self,n=10):
        from nltk.collocations import BigramCollocationFinder
        from nltk.metrics import BigramAssocMeasures
        from nltk.corpus import stopwords
        rows = []
        self.pull_corpus_as_words() 
        stopset = set(stopwords.words('english'))
        filter_stops = lambda w: len(w) < 3 or w in stopset
        finder = BigramCollocationFinder.from_words(self.doc_words)
        finder.apply_word_filter(filter_stops)
        finder.apply_freq_filter(3)
        bigram_measures = nltk.collocations.BigramAssocMeasures()
        scored = finder.score_ngrams(bigram_measures.raw_freq)
        for bigram, score in scored:
            rows.append([bigram[0],bigram[1],score])
        with self.dbi as db:
            db.insert_values('bigram',rows)

    def pull_gensim_corpus(self):
        with self.dbi as db:
            n = db.cur.execute("SELECT count(*) FROM docword").fetchone()[0]
            self.gensim_corpus = [[] for _ in range(n)]
            for r in db.cur.execute('SELECT doc_index, word_id, word_count FROM docword ORDER BY doc_id, word_id'):
                self.gensim_corpus[r[0]].append((r[1], r[2]))

    def pull_gensim_token2id(self):
        self.gensim_token2id = {}
        with self.dbi as db:
            for r in db.cur.execute("SELECT word_str,word_id FROM word"):
                self.gensim_token2id[r[0]] = r[1]

    def pull_gensim_id2token(self):
        self.gensim_id2token = {}
        with self.dbi as db:
            for r in db.cur.execute("SELECT word_str,word_id FROM word"):
                self.gensim_id2token[r[1]] = r[0]
                
    def generate_mallet_corpus(self, outfile):
        with self.dbi as db, open(outfile,'w') as out:
            for r in db.cur.execute("SELECT doc_id || '', doc_label, doc_str FROM doc"):
                out.write(','.join(r)+'\n')
        
if __name__ == '__main__':

    print('This is an abstract class. Override src_doc_generator in another class to use it.')
