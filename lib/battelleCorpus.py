import re, sqlite3
from corpus import Corpus


'''
This corpus subclass expects to find a comma-delimitted corpus file
with three columns -- a unique doc_id, a doc_tag, and doc_content 
'''

class BattelleCorpus(Corpus):
    
    def __init__(self,src_db,dst_db,dictfile=None,nltk_data_path=None):
        self.src_db = src_db
        Corpus.__init__(self,dst_db,dictfile,nltk_data_path)

    def src_doc_generator(self):
        with sqlite3.connect(self.src_db) as conn:
            cur = conn.cursor()
            for r in cur.execute("SELECT doc_id, year as 'doc_label', abstract as 'doc_str' FROM src_all_doi ORDER BY doc_id"):
                yield r
                