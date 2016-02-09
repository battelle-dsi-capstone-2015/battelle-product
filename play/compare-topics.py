import pandas as pd, math
from time import time
import scipy as sp
import scipy.stats
import numpy as np

def JSdivergence(p1,p2):
    P1 = p1/np.sum(p1)
    P2 = p2/np.sum(p2)
    M = .5*(P1+P2)
    return .5*(sp.stats.entropy(P1,M) + sp.stats.entropy(P2,M))
    
def create_topicpair_sql():
    df = pd.read_csv('topicword.txt');
    z = 200
    n = 0
    t = z**2 - ((z**2 + z)/2)
    with open('topicpairs.sql','w') as out:
        out.write('DROP TABLE IF EXISTS topicpair;\n')
        out.write('CREATE TABLE topicpair (topic_id1 INTEGER, topic_id2 INTEGER, cosine_sim REAL, js_div REAL);\n')
        for i in range(z):
            x = df['t%s' % i]
            c1 = math.sqrt(sum([m * m for m in x]))
            for j in range(i+1,z):
                y = df['t%s' % j]
                c2 = math.sqrt(sum([m * m for m in y]))
                c3 = math.sqrt(sum([m * n for m,n in zip(x,y)]))
                c4 = c3 / (c1 * c2)
                jsd = JSdivergence(x,y)
                out.write('INSERT INTO topicpair (topic_id1,topic_id2,cosine_sim,jsd_div) VALUES ({},{},{},{});\n'.format(i,j,c4,jsd))
                n += 1
                percent = round(n/t,2) * 100
                print(percent,i,j)
        
if __name__ == '__main__':
    create_topicpair_sql()
