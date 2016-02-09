'''
We create a network of documents via topics based on the simple
principle that two documents are related if they shared a dominant
topic. In other words, top topics act as containers in the CCC (common
container correlation) patter. For simplicity, we only capture the top
to topics for each documents, and we do not consider the weight of the
topic in each case.
'''

import sqlite3

sql = '''
SELECT doc_id, topic_id
FROM doctopic_long
WHERE doc_id = ?
ORDER by doc_id, topic_weight DESC 
LIMIT 2
'''
dbin = '../webapp/battelle.db'
dbout = 'docnet-test.db'
with sqlite3.connect(dbin) as connin, sqlite3.connect(dbout) as connout:
    curout = connout.cursor()
    curout.execute('DROP TABLE IF EXISTS docnet')
    curout.execute('CREATE TABLE docnet (doc_id INTEGER, topic_id INTEGER, UNIQUE(doc_id, topic_id))')
    connout.commit()
    curin = connin.cursor()
    for doc_id in [r[0] for r in curin.execute('SELECT doc_id FROM doc')]:
        for r in curin.execute(sql,[doc_id]):
            curout.execute('insert into docnet (doc_id,topic_id) values (?,?)',r)
    connout.commit()

