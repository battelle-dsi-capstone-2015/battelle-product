#!/usr/bin/env python

'''
This script simply grabs docs from each of the src tables and filters them
to produce an aggregrate table. We filter for presence of DOIs and a few
thing like if there exists an abstract. Duplicates are excluded, as we take
just the first doc that has a give DOI. Note that the generated table --
src_all_doi -- is intended to replace src_all.
'''
import sqlite3

srcs = {
    'engineeringvillage': {
        'authors': 'Author',
        'affiliations': 'Author affiliation',
        'title': 'Title',
        'year': 'Publication year',
        'abstract': 'Abstract',
        'doi': 'DOI',
        'cite_count': 'Number of references'
    },
    'ieee':{
        'authors': 'Authors',
        'affiliations': 'Author Affiliations',
        'title': 'Document Title',
        'year': 'Year',
        'abstract': 'Abstract',
        'doi': 'DOI',
        'cite_count': 'Reference Count',
    },
    'isi':{
        'authors': 'AU',
        'affiliations': 'C1',
        'title': 'TI',
        'year': 'PY',
        'abstract': 'AB',
        'doi': 'DI',
        'cite_count': 'NR'
    },
    'scopus':{
        'authors': 'Authors',
        'affiliations': 'Affiliations',
        'title': 'Title',
        'year': 'Year',
        'abstract': 'Abstract',
        'doi': 'DOI',
        'cite_count': 'Cited by'
    }
}

def aggregate_sources(dbfile):
    with sqlite3.connect(dbfile) as conn:
        cur1 = conn.cursor()
        cur2 = conn.cursor()
        cur1.execute('DROP TABLE IF EXISTS src_all_doi')
        cur1.execute('CREATE TABLE src_all_doi (doc_id INTEGER PRIMARY KEY, authors TEXT,affiliations TEXT,title TEXT,year INTEGER,abstract TEXT,doi TEXT,cite_count INTEGER,src TEXT)')
        conn.commit()
        doi_list = []
        doc_id = 0
        for src in srcs:
            select_str = ','.join(['"{}"'.format(srcs[src][k]) for k in sorted(srcs[src])])
            for r in cur1.execute('SELECT {} FROM src_{}'.format(select_str,src)):
                (abstract,affiliations,authors,cite_count,doi,title,year) = r
                try:
                    if int(year) not in range(2005,2016): 
                        continue
                except:
                    continue
                if 'This article has been retracted by the publisher' in abstract:
                    continue
                if 'No abstract available' in abstract: 
                    continue
                if 'Notice of Retraction' in abstract:
                    continue
                if abstract == '':
                    continue
                if doi == '':
                    continue
                if doi not in doi_list:
                    doi_list.append(doi)
                    doc_id += 1
                    try:
                        cite_count = int(cite_count) # cite_count
                    except:
                        cite_count = None
                    year = int(year)
                    values = [doc_id,abstract,affiliations,authors,cite_count,doi,title,year,src]
                    cur2.execute('INSERT INTO src_all_doi (doc_id,abstract,affiliations,authors,cite_count,doi,title,year,src) VALUES (?,?,?,?,?,?,?,?,?)',values)
        conn.commit()
        cur1.close()
        cur2.close()
                 
if __name__ == '__main__':
    
    aggregate_sources('battelle-sources.db')
    
