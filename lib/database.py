import sqlite3, sys

class Database:

    '''
    db_file = 'somefile.db' # Path to a SQLite3 file
    db_schema = {
        'table1': ('field1 TYPE1','field2 TYPE2') # Sample
    }
    '''
    
    def __init__(self,db_file,db_schema):
        self.db_file = db_file
        self.db_schema = db_schema
        try:
            self.conn = sqlite3.connect(self.db_file)
        except sqlite3.Error as e:
            print("Can't connect to database:", e.args[0])
            sys.exit(0)

    def __del__(self):
        try:
            self.conn.close()
        except sqlite3.Error as e:
            print("Can't close database:", e.args[0])

    def __enter__(self):
        self.cur = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.commit()
        self.cur.close()

    def expand_fields(self,field_prefix,field_count,field_type=None):
        return ['{}{} {}'.format(field_prefix, i, field_type).lower().strip() for i in range(field_count)]

    '''
    def expand_fields_in_schema(self,field_prefix,field_count):
        for table in self.db_schema:
            fields = []
            for field in self.db_schema[table]:
                (this_field_name, this_field_type) = field.split()
                if this_field_name == '_topics_':
                    for i in range(field_count):
                        fields.append('{}{} {}'.format(field_prefix, i, field_type).strip())
                else:
                    fields.append(field)
    '''
    
    # The following methods must be called in the context of the WITH
    # statement defined by the above enter and exit methods.

    def create_table(self,table):
        self.cur.execute('DROP TABLE IF EXISTS {}'.format(table))
        self.cur.execute('CREATE TABLE {} ({})'.format(table, ','.join(self.db_schema[table])))

    def insert_values(self,table,values):
        self.create_table(table)
        places = ','.join(['?' for _ in range(len(self.db_schema[table]))])
        sql = 'INSERT INTO {} VALUES ({})'.format(table,places)
        for value in values:
            self.cur.execute(sql,value)

    def select_values(self,table):
        fields = [field.split()[0] for field in self.db_schema[table]]
        for r in self.cur.execute('SELECT {} FROM {}'.format(','.join(fields), table)):
            yield r
