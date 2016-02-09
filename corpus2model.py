#! /usr/bin/env python

import sys; sys.path.append('lib')
import Mallet 

mc = Mallet.Config()
mc.mallet_path     = '/etc/mallet/bin/mallet'
mc.output_dir      = 'models'
mc.input_corpus    = 'models/input/mallet-corpus.csv'
mc.extra_stops     = 'models/input/extra-stops.csv'
mc.label           = 'battelle'
mc.num_topics      = 200
mc.num_iterations  = 1000
mc.verbose         = False

mi = Mallet.Interface(mc)
#m.config.trial_name = '' # Assign this to work with an existing trial (and comment out import() and train() below

print('Initializing mallet')
mi.mallet_init()
print('Importing corpus')
mi.mallet_import()
print('Training model')
mi.mallet_train()

mm = Mallet.Model(mi)
print('Populating tables')
mm.populate_model()

mm.generate_dbfilename()
print(mm.dbfilename)

# Post processing extras
import mallet_utils as mu
print('Creating doctopic_long')
mu.create_doctopic_long_table(mm.dbfilename)
print('Creating topicword_long')
mu.create_topicword_long_table(mm.dbfilename)

sys.exit(0)