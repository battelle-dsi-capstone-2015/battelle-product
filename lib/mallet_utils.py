import sqlite3, math

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
'''
def create_topicpair_table(z,model_db):
    
    with sqlite3.connect(model_db) as conn:
        cur1 = conn.cursor()
        #cur2 = conn.cursor()
        #cur1.execute('DROP TABLE IF EXISTS topicpair')
        #cur1.execute('CREATE TABLE topicpair (topic_id1 INTEGER, topic_id2 INTEGER, cosine_sim REAL, UNIQUE (topic_id1, topic_id2))')
        #conn.commit()
        for i in range(z):
            for j in range(i+1,z):
                sql = "SELECT  SUM(t{0} * t{1}), SUM(t{0} * t{0}), SUM(t{1} * t{1}) FROM topicword".format(i,j)
                cur1.execute(sql)
                r = cur1.fetchone()
                cosine_sim = r[0] / (math.sqrt(r[1]) * math.sqrt(r[2]))
                print(i,j,cosine_sim)
                #cur2.execute("INSERT INTO topicpair VALUES (?,?,?)",(i,j,cosine_sim))
        #conn.commit()
'''