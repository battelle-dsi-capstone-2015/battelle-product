import sqlite3
import numpy as np
import scipy.stats as sps

class CorpusStats:

    model = {
        'corpusstats' : {
            'cols':['rowid INTEGER','mean_topic_entropy REAL','max_topic_entropy REAL','min_topic_entropy REAL'], 
            'keys':['PRIMARY KEY(rowid)']},
        'docstats': {
            'cols':['doc_id INTEGER','topic_entropy REAL','doc_size INTEGER'],
            'keys':['PRIMARY KEY(doc_id)']},
        'wordstats': {
            'cols':['word_id INTEGER'],
            'keys':['PRIMARY KEY(word_id)']},
        'topicstats': {
            'cols':['topic_id INTEGER'], 
            'keys':['PRIMARY KEY(topic_id)']},
        'topicnet': {
            'cols':['src_topic INTEGER','dst_topic INTEGER','doc_label TEXT','n INTEGER'],
            'keys':['UNIQUE(src_topic,dst_topic,doc_label)']},
        'topicpairsinglesdoc': {
            'cols':['src_topic INTEGER','dst_topic INTEGER','doc_count'],
            'keys':['UNIQUE(src_topic,dst_topic)']}
        }

    def __init__(self,dbin,dbout):
        self.connin = sqlite3.connect(dbin)
        self.curin = self.connin.cursor()
        self.connout = sqlite3.connect(dbout)
        self.curout = self.connout.cursor()

    def create_database(self):
        for table in self.model:
            cols = ','.join([col for col in self.model[table]['cols']])
            keys = ','.join([key for key in self.model[table]['keys']])
            sql1 = 'DROP TABLE IF EXISTS {}'.format(table)
            sql2 = 'CREATE TABLE {} ({},{})'.format(table,cols,keys)
            self.curout.execute(sql1)
            self.curout.execute(sql2)
            self.connout.commit()
            
    def topic_network(self):
        sql1 = '''
        SELECT a.topic_id, b.topic_id, c.doc_label, count(*) as 'n' 
        FROM doctopic_long a 
            JOIN doctopic_long b USING (doc_id)
            JOIN doc c ON (a.doc_id = c.doc_id)
            JOIN doc d ON (b.doc_id = d.doc_id)
        WHERE a.topic_weight >= 0.1 
            AND b.topic_weight >= 0.1
            AND a.topic_id != b.topic_id
        GROUP BY a.topic_id, b.topic_id, c.doc_label
        '''
        sql2 = "INSERT INTO topicnet VALUES (?,?,?,?)"
        for r in self.curin.execute(sql1):
            self.curout.execute(sql2,r)
        self.connout.commit()
        
    def topic_pair_singles(self):
        
        # Grap topic pairs that appear only once in our time series
        sql0 = '''
        ATTACH "/lv1/battelle-dev/PRODUCT/webapp/battelle.db" AS battelle;
        SELECT src_topic, st.topic_words as 'src_words', st.topic_alpha as 'src_alpha', dst_topic, dt.topic_words as 'dst_words', dt.topic_alpha as 'dst_alpha', count(*) as 'n' 
        FROM "topicnet" 
        JOIN battelle.topic st ON (src_topic = st.topic_id)
        JOIN battelle.topic dt ON (dst_topic = dt.topic_id)
        WHERE n = 1
        GROUP BY src_topic, dst_topic 
        ORDER BY (src_alpha + dst_alpha)
        '''
        
        sql1 = '''
        SELECT src_topic, dst_topic, count(*) as 'n' 
        FROM topicnet 
        WHERE n = 1
        GROUP BY src_topic, dst_topic 
        '''
        pairs = [r[:2] for r in self.curout.execute(sql1) if r[2] == 1]
        
        # Grab the number of documents associated with each 
        
        sql3 = "INSERT INTO topicpairsinglesdoc VALUES (?,?,?)"
        
        for pair in pairs:
            sql2 = "SELECT count(*) as 'n' FROM doctopic WHERE t{} >= 0.1 AND t{} >= 0.1".format(pair[0],pair[1])
            for r in self.curin.execute(sql2):
                values = (pair[0],pair[1],r[0])
                print(values)
                self.curout.execute(sql3,values)
            self.connout.commit()   

    def topic_pair_list(self):
        sql1 = "SELECT src_topic, dst_topic, n FROM topicnet ORDER BY n DESC"
        for r in self.curout.execute(sql1):
            print(r)
        
    def topic_matrix(self):
        sql1 = "SELECT src_topic, dst_topic, n FROM topicnet"
        tnet = {}
        for r in self.curout.execute(sql1):
            st,dt,n = r
            if st not in tnet.keys():
                tnet[st] = {}
            try:
                _ = tnet[dt][st]
            except:
                tnet[st][dt] = n
        z = len(tnet)
        
        max = [r[0] for r in self.curout.execute('SELECT MAX(n) FROM topicnet')][0]

        # Create table with color showing weight
        print("<table border=1 cellspacing=0>")
        header = ''.join(["<td>t{}</td>".format(i) for i in range(z)])
        print("<th>{}</th>".format(header));
        for i in range(z):
            print("<tr><td>t{}</td>".format(i))
            for j in range(z):
                v  = '&nbsp;'
                x = 0
                try:
                    v = tnet[i][j]
                    x = v/max
                except:
                    pass    
                print("<td style='width:30px;height:30px;'>{}</td>".format(x))
            print("</tr>")
        print("</table>")

    def topic_entropy(self):

        # Get stats for the whole corpus (sample)
        cstats = {}
        sql1 = "SELECT avg(topic_entropy), max(topic_entropy), min(topic_entropy) FROM doctopic"
        sql2 = "INSERT INTO corpusstats (mean_topic_entropy,max_topic_entropy,min_topic_entropy) VALUES (?,?,?)"
        for r in self.curin.execute(sql1):
            cstats['avg_h'], cstats['max_h'], cstats['min_h'] = r
            self.curout.execute(sql2,r)
        self.connout.commit()
        
        # Get stats for each doc (trial, observation)
        dstats = {}
        all_h = []
        sql3 = "SELECT doc_id, doc_label, topic_entropy FROM doctopic"
        for r in self.curin.execute(sql3):
            dstats[r[0]] = {}
            dstats[r[0]]['label'] = r[1]
            dstats[r[0]]['entropy'] = r[2]
            all_h.append(r[2])
            
        all_h = np.array(all_h)
        dr = sps.describe(all_h)
        print(dr)
        for k in dr:
            print(k)
        
if __name__ == '__main__':
    
    dbin = '../webapp/battelle.db'
    dbout = 'docstats.db'

    cs = CorpusStats(dbin,dbout)
    
    print('(Re)creating the database')
    cs.create_database()
    #cs.topic_entropy()
    
    print("Generating topic network")
    cs.topic_network()
    
    print("Getting doc counts for topic pair singles")
    cs.topic_pair_singles()
    
    #cs.topic_matrix()
