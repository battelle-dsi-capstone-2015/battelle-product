import sqlite3
import math

def create_doctopic_long_table(model_db):
    with sqlite3.connect(model_db) as conn:
        cur1 = conn.cursor()
        cur2 = conn.cursor()
        cur1.execute('DROP TABLE IF EXISTS doctopic_long')
        cur1.execute('CREATE TABLE doctopic_long(doc_id INTEGER NOT NULL, topic_id INTEGER NOT NULL, topic_weight REAL NOT NULL, UNIQUE (doc_id, topic_id))')
        conn.commit()
        for r in cur1.execute("SELECT * FROM doctopic"):
            doc_id = r[0]
            if (doc_id % 1000 == 0):
                print(doc_id)
            doc_label = r[1]
            topic_entropy = r[2]
            for i,v in enumerate(r[3:]):
                cur2.execute("INSERT INTO doctopic_long (doc_id,topic_id,topic_weight) VALUES (?,?,?)",(doc_id,i,v))
        conn.commit()
        
def create_topicword_long_table(model_db):
    with sqlite3.connect(model_db) as conn:
        cur1 = conn.cursor()
        cur2 = conn.cursor()
        cur1.execute('DROP TABLE IF EXISTS topicword_long')
        cur1.execute('CREATE TABLE topicword_long (word_id INTEGER NOT NULL, word_str TEXT NOT NULL, topic_id INTEGER NOT NULL, word_count INTEGER NOT NULL, UNIQUE (word_id, topic_id))')
        conn.commit()
        for r in cur1.execute("SELECT * FROM topicword ORDER BY word_id"):
            word_id = r[0]
            if (word_id % 1000 == 0):
                print(word_id)
            word_str = r[1]
            for i,v in enumerate(r[2:-1]):
                if v > 0:
                    cur2.execute("INSERT INTO topicword_long (word_id,word_str,topic_id,word_count) VALUES (?,?,?,?)",(word_id,word_str,i,v))
        conn.commit()
        
import pandas as pd
from time import time
import scipy as sp
import scipy.stats
import numpy as np

def JSdivergence(p1,p2):
    P1 = p1/np.sum(p1)
    P2 = p2/np.sum(p2)
    M = .5*(P1+P2)
    return .5*(sp.stats.entropy(P1,M) + sp.stats.entropy(P2,M))

def create_topicpair_table(model_db,stats_db):
    with sqlite3.connect(model_db) as conn1, sqlite3.connect(stats_db) as conn2:        
        
        df = pd.read_sql('SELECT * FROM topicword',conn1);
        
        cur1 = conn1.cursor()
        cur1.execute("SELECT value FROM config WHERE key = 'num_topics'")
        r = cur1.fetchone()
        z = int(r[0])
        n = 0
        t = z**2 - ((z**2 + z)/2)

        cur2 = conn2.cursor()
        cur2.execute('DROP TABLE IF EXISTS topicpair')
        cur2.execute('CREATE TABLE topicpair (topic_id1 INTEGER, topic_id2 INTEGER, cosine_sim REAL, js_div REAL)')
        conn2.commit() 
        for i in range(z):
            x = df['t{}'.format(i)]
            c1 = math.sqrt(sum([m * m for m in x]))
            for j in range(i+1,z):
                y = df['t{}'.format(j)]
                c2 = math.sqrt(sum([m * m for m in y]))
                c3 = math.sqrt(sum([m * n for m,n in zip(x,y)]))
                c4 = c3 / (c1 * c2)
                jsd = JSdivergence(x,y)
                cur2.execute('INSERT INTO topicpair (topic_id1,topic_id2,cosine_sim,js_div) VALUES (?,?,?,?)', (i,j,c4,jsd))
                n += 1
                percent = round(n/t,2) * 100
                print(percent,i,j)
            conn2.commit()
        
            
def create_docpair_table(mode_db,stats_db):
    # To be added 
    pass

'''
TO DO:
-- Create a function to get topicpairs by conditional dependency for each year
-- Create visualizations of positive, negative, and neutral dependency ...
'''
def create_topicpair_by_deps_table(model_db,stats_db):
    with sqlite3.connect(model_db) as conn1, sqlite3.connect(stats_db) as conn2:        

        doctopics   = pd.read_sql('SELECT * FROM doctopic', conn1)
        topics      = pd.read_sql('SELECT * FROM topic', conn1)
        n           = doctopics.doc_id.count()
        z           = topics.topic_id.count()
        
        tw_min      = 0.1

        cur2 = conn2.cursor()
        cur2.execute('DROP TABLE IF EXISTS topicpair_by_deps')
        cur2.execute('CREATE TABLE topicpair_by_deps (topic_a INTEGER, topic_b INTEGER, p_a REAL, p_b REAL, p_ab REAL, p_aGb REAL, p_bGa REAL)')
        conn2.commit() 
        
        for i in range(z):
            sel_i = doctopics['t{}'.format(i)] >= tw_min
            n_i = doctopics[sel_i].doc_id.count()    
            p_i = n_i / n
        
            for j in range(i+1,z):
                sel_j = doctopics['t{}'.format(j)] >= tw_min
                n_j = doctopics[sel_j].doc_id.count()
                p_j = n_j / n
                n_ij = doctopics[sel_i & sel_j].doc_id.count()
                p_iAj = n_ij / n
                p_jGi = n_ij / n_i
                p_iGj = n_ij / n_j
                cur2.execute('INSERT INTO topicpair_by_deps (topic_a, topic_b, p_a, p_b, p_ab, p_aGb, p_bGa) VALUES (?,?,?,?,?,?,?)',(i,j,p_i,p_j,p_iAj,p_iGj,p_jGi))
                    
            conn2.commit()
        
