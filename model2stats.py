import sys; sys.path.append('lib')
import mallet_utils as mu

model_db = 'models/battelle-model-z200-i1000-1454677957.db'
stats_db = 'stats/stats.db'

#print('Creating topicpair')
#mu.create_topicpair_table(model_db,stats_db)

print('Creating topicpair_by_deps')
mu.create_topicpair_by_deps_table(model_db,stats_db)

sys.exit(0)