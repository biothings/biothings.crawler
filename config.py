''' 
Biothings API Web Config for
http://crawler.biothings.io/
'''

from web.pipeline import MPQueryBuilder
from web.pipeline import MPResultTransformer

# *****************************************************************************
# Elasticsearch
# *****************************************************************************
ES_DOC_TYPE = '_all'
ES_HOST = 'localhost:9200'
ES_INDEX = 'indexed_*'
ES_INDICES = {
    'dataverse':'indexed_harvard_*',
    'discovery':'indexed_discovery',
    'omicsdi':'indexed_omicsdi',
    'zenodo':'indexed_zenodo',
    'immport':'indexed_immport',
    'nyu':'indexed_nyu',
    'geo':'indexed_ncbi*',
}
# *****************************************************************************
# Tornado URL Patterns
# *****************************************************************************
API_PREFIX = 'api'
API_VERSION = ''
TEMPLATE_PATH = 'static'
# *****************************************************************************
# Biothings SDK Settings
# *****************************************************************************
ES_QUERY_BUILDER = MPQueryBuilder
ES_RESULT_TRANSFORM = MPResultTransformer
ALLOW_RANDOM_QUERY = True
