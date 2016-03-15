import os, configparser, subprocess, sys, sqlite3, re, time
from math import log
from lxml import etree

class Config:
    num_top_words       = 7
    mallet_path         = 'mallet'
    output_dir          = '.'
    input_corpus        = 'corpus.csv'
    extra_stops         = 'extra-stopwords.csv'
    label               = 'foo'
    num_topics          = 20
    num_iterations      = 100
    num_top_words       = 7
    optimize_interval   = 10
    num_threads         = 1
    verbose             = False    

class Interface:
    def __init__(self,config):
        self.config = config
        self.generate_trial_name()
        
    def generate_trial_name(self):
        ts = time.time()
        self.config.trial_name = '{}-model-z{}-i{}-{}'.format(self.config.label,self.config.num_topics,self.config.num_iterations,int(ts))
        
    def mallet_init(self):
        if not os.path.exists(self.config.mallet_path):
            print('OOPS Mallet cannot be found')
            sys.exit(0)
        
        self.mallet = {'import-file':{}, 'train-topics':{}}
        if os.path.exists(self.config.extra_stops):
            self.mallet['import-file']['extra-stopwords'] = self.config.extra_stops
        self.mallet['import-file']['input'] = self.config.input_corpus
        self.mallet['import-file']['output'] = '%s/%s-corpus.mallet' % (self.config.output_dir,self.config.label)
        self.mallet['import-file']['keep-sequence'] = '' # Delete key to remove option
        self.mallet['import-file']['remove-stopwords'] = '' # Delete key to remove option
        self.mallet['train-topics']['num-topics'] = self.config.num_topics
        self.mallet['train-topics']['num-top-words'] = self.config.num_top_words
        self.mallet['train-topics']['num-iterations'] = self.config.num_iterations
        self.mallet['train-topics']['optimize-interval'] = self.config.optimize_interval
        self.mallet['train-topics']['num-threads'] = self.config.num_threads
        self.mallet['train-topics']['input'] = self.mallet['import-file']['output']
        self.mallet['train-topics']['output-topic-keys'] = '%s/%s-topic-keys.txt' % (self.config.output_dir,self.config.trial_name)
        self.mallet['train-topics']['output-doc-topics'] = '%s/%s-doc-topics.txt' % (self.config.output_dir,self.config.trial_name)
        self.mallet['train-topics']['word-topic-counts-file'] = '%s/%s-word-topic-counts.txt' % (self.config.output_dir,self.config.trial_name)
        self.mallet['train-topics']['xml-topic-report'] = '%s/%s-topic-report.xml' % (self.config.output_dir,self.config.trial_name)
        self.mallet['train-topics']['xml-topic-phrase-report'] = '%s/%s-topic-phrase-report.xml' % (self.config.output_dir,self.config.trial_name)

    def mallet_run_command(self,op):
        my_args = ['--{} {}'.format(arg,self.mallet[op][arg]) for arg in self.mallet[op]]
        self.mallet_output = subprocess.check_output([self.config.mallet_path, op] + my_args, shell=False)
        
    def mallet_import(self):
        self.mallet_run_command('import-file')

    def mallet_train(self):
        self.mallet_run_command('train-topics')

class Table:
    
    src_file_path = None
    
    def __init__(self,name,raw_fields,z=0):
        self.name = name
        self.raw_fields = raw_fields
        self.z = z
        self.tn_list = ['t{}'.format(tn) for tn in range(int(self.z))]
        self.get_field_defs()
        self.get_sql_def() 
        
    def get_field_defs(self):
        self.field_defs = []
        fields = [] # To use in the field string for INSERTs
        for field in self.raw_fields:
            if field[0] == '_topics_':
                t_type = field[1]
                for i in range(int(self.z)):
                    self.field_defs.append('{} {}'.format(self.tn_list[i],t_type))
                    fields.append(self.tn_list[i])
            else:
                self.field_defs.append(' '.join(field))
                fields.append(field[0])
        # Also create the field string for INSERTs
        field_str = ','.join(fields)
        value_str = ','.join(['?' for _ in range(len(fields))])
        self.insert_sql = 'INSERT INTO {} ({}) VALUES ({})'.format(self.name,field_str,value_str)
        
    def get_field_dict(self):
        self.field_dict = {}
        for field_def in self.field_defs:
            k, v = field_def.split(' ')
            self.field_dict[k] = v
                
    def get_sql_def(self):
        self.sql_def = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(self.name, ', '.join(self.field_defs))
    
    def create_table(self,conn):
        cur = conn.cursor()
        cur.execute('DROP TABLE IF EXISTS {}'.format(self.name))
        cur.execute(self.sql_def)
        conn.commit()
        cur.close()
    
    def src_data_iter(self):
        with open(self.src_file_path) as src_file:
            for line in src_file.readlines(): 
                yield line.strip()

    # This is an abstract class            
    def import_src_data(self,conn):
        pass
            
class Model:
    
    tables = {
        #'doc': object,
        'topic': object,
        'doctopic': object,
        'topicword': object,
        'topicphrase': object,
        'config': object
    }
        
    def __init__(self,mallet_interface):
        
        self.config = mallet_interface.config
        self.mallet = mallet_interface.mallet

        self.z = self.config.num_topics
        self.generate_dbfilename()
        
        #self.tables['doc'] = self.DocTable()
        self.tables['topic'] = self.TopicTable()
        self.tables['doctopic'] = self.DocTopicTable(self.z)
        self.tables['topicword'] = self.TopicWordTable(self.z)
        self.tables['topicphrase'] = self.TopicPhraseTable()
        self.tables['config'] = self.ConfigTable(mallet_interface) # Change this when switching to MalletConfig

        #self.tables['doc'].src_file_path = self.config.input_corpus
        self.tables['topic'].src_file_path = self.mallet['train-topics']['output-topic-keys']
        self.tables['doctopic'].src_file_path = self.mallet['train-topics']['output-doc-topics']
        self.tables['topicword'].src_file_path = self.mallet['train-topics']['word-topic-counts-file']        
        self.tables['topicphrase'].src_file_path = self.mallet['train-topics']['xml-topic-phrase-report']
        
    '''
    class DocTable(Table):
        
        raw_fields = (('doc_id','TEXT'), ('doc_label','TEXT'), ('doc_content','TEXT'))

        def __init__(self):
            Table.__init__(self,'doc',self.raw_fields)
            
        def import_src_data(self, conn):
            self.create_table(conn)
            cur = conn.cursor()
            for line in self.src_data_iter():
                values = []
                row = line.split(',')
                values.append(row[0]) # doc_id
                values.append(row[1]) # doc_label
                values.append(row[2]) # doc_content
                cur.execute(self.insert_sql,values)
            conn.commit()
            cur.close()
    '''
            
    class TopicTable(Table):
        
        raw_fields = (('topic_id', 'INTEGER PRIMARY KEY'), ('topic_alpha', 'REAL'), ('total_tokens', 'INTEGER'), ('topic_words', 'TEXT'))
        
        def __init__(self):
            Table.__init__(self,'topic',self.raw_fields)

        def import_src_data(self, conn):
            self.create_table(conn)
            cur = conn.cursor()
            for line in self.src_data_iter():
                values = []
                row = line.split('\t')
                values.append(row[0]) # topic_id <-- SHOULD THIS USE self.tn_list ?
                values.append(row[1]) # topic_alpha
                values.append(0) # Place holder for total_tokens until XML file is handled
                values.append(row[2]) # topic_list
                cur.execute(self.insert_sql,values)
            conn.commit()
            cur.close()

    class DocTopicTable(Table):
        
        raw_fields = (('doc_id', 'INTEGER PRIMARY KEY'), ('doc_label', 'TEXT'), ('topic_entropy', 'REAL'), ('_topics_', 'REAL'))
        
        def __init__(self,z):
            Table.__init__(self,'doctopic',self.raw_fields,z)

        def import_src_data(self, conn):
            self.create_table(conn)
            cur = conn.cursor()
            for line in self.src_data_iter():
                if re.match('^#',line):
                    continue
                values = []
                row = line.split('\t')
                info = row[1].split(',') 
                values.append(info[0]) # doc_id
                values.append(info[1]) # doc_label
                
                H = 0 # Entropy
                tws = [0 for i in range(int(self.z))]

                # Determine how many cols, since MALLET does it two ways ...
                # Shouldn't have to do this for each row, though
                # Should get the row lenght and calculate type once
                src_type = len(row) - 2
                
                # Type A -- Topic weights in order of topic number
                if (src_type == int(self.z)):
                    for i in range(int(self.z)):
                        tn = i
                        tw = float(row[i+2])
                        tws[tn] = tw
                        if tw != 0:
                            H += tw * log(tw)
                
                # Type B -- Topic weights in order to weight, with topic number paired
                elif (src_type == int(self.z) * 2):
                    for i in range(int(self.z)*2,2):
                        tn = int(row[i+2])
                        tw = float(row[i+3])
                        tws[tn] = tw
                        if tw != 0:
                            H += tw * log(tw)

                else:
                    print('NOT SURE WHAT KIND OF DOCTOPIC TABLE THIS IS')
                        
                values.append(-1 * H) # topic_entropy
                for tw in tws:
                    values.append(tw) # topic weights (t1 ... tn)
                    
                cur.execute(self.insert_sql,values)
            conn.commit()
            cur.close()
    
    class TopicWordTable(Table):
        
        raw_fields = (('word_id', 'INTEGER'), ('word_str', 'TEXT'), ('_topics_', 'INTEGER'),('word_sum','INTEGER'))
        
        def __init__(self,z):
            Table.__init__(self,'topicword',self.raw_fields,z)

        def import_src_data(self, conn):
            self.create_table(conn)
            cur = conn.cursor()
            for line in self.src_data_iter():
                values = []
                row = line.split(' ')
                values.append(row[0]) # word_id
                values.append(row[1]) # word_str
                counts = {} # word_counts
                word_sum = 0
                for x in row[2:]:
                    y = x.split(':') # y[0] = topic num, y[1] = word count
                    counts[str(y[0])] = y[1]
                for i in range(int(self.z)):
                    tn = str(i)
                    if tn in counts.keys():
                        word_sum += int(counts[tn])
                        values.append(counts[tn])
                    else:
                        values.append(0)
                values.append(word_sum)
                cur.execute(self.insert_sql,values)
            conn.commit()
            cur.close()

    class TopicPhraseTable(Table):
        
        raw_fields = (('topic_id', 'INTEGER'), ('topic_phrase', 'TEXT'), ('phrase_count', 'INTEGER'), ('phrase_weight', 'REAL'))
        
        def __init__(self):
            Table.__init__(self,'topicphrase',self.raw_fields)

        def import_src_data(self, conn):
            self.create_table(conn)
            cur = conn.cursor()
            with open(self.src_file_path) as fd:
                tree = etree.parse(fd)
                for topic in tree.xpath('/topics/topic'):
                    topic_id = topic.xpath('@id')[0]
                    total_tokens = topic.xpath('@totalTokens')[0]
                    sql1 = "UPDATE topic SET total_tokens = ? WHERE topic_id = ?" # Risky
                    cur.execute(sql1,[total_tokens,topic_id])                            
                    for phrase in topic.xpath('phrase'):
                        values = []
                        phrase_weight = phrase.xpath('@weight')[0]
                        phrase_count = phrase.xpath('@count')[0]
                        topic_phrase = phrase.xpath('text()')[0]
                        values.append(topic_id)
                        values.append(topic_phrase)
                        values.append(phrase_count)
                        values.append(phrase_weight)
                        cur.execute(self.insert_sql,values)
            conn.commit()
            cur.close()
            
    class ConfigTable(Table):
        
        raw_fields = (('key','TEXT'),('value','TEXT'))        
        
        def __init__(self,mallet_interface):
            Table.__init__(self,'config',self.raw_fields)
            self.mallet = mallet_interface.mallet
            self.config = mallet_interface.config

        def import_src_data(self, conn):
            self.create_table(conn)
            cur = conn.cursor()
            for k,v in self.config.__dict__.items():
                cur.execute('INSERT INTO config VALUES (?,?)',[k,v])
            for k1 in self.mallet:
                for k2 in self.mallet[k1]:
                    key = re.sub('-', '_', 'mallet_{}_{}'.format(k1,k2))
                    val = self.mallet[k1][k2]
                    cur.execute('INSERT INTO config VALUES (?,?)',[key,val])
            conn.commit()
    
    def populate_model(self):
        with sqlite3.connect(self.dbfilename) as conn:
            for table in sorted(self.tables):
                self.tables[table].import_src_data(conn)
    
    def generate_dbfilename(self):
        self.dbfilename = '{}/{}.db'.format(self.config.output_dir,self.config.trial_name)

if __name__ == '__main__':

    print('Not here, not now.')


