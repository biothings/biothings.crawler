
from biothings.web.settings.default import *
from biothings.web.api.es.handlers import QueryHandler
from web.ncbi_geo import APP_LIST as GEO_APP

# *****************************************************************************
# Elasticsearch
# *****************************************************************************
ES_INDEX = 'ncbi_geo_indexed'
ES_DOC_TYPE = '_doc'
ES_HOST = ['localhost:9199']

# *****************************************************************************
# Tornado URL Patterns
# *****************************************************************************
UNINITIALIZED_APP_LIST = GEO_APP
APP_LIST = [
    (r"/api/query/?", QueryHandler),
]

# *****************************************************************************
# Biothings SDK Settings
# *****************************************************************************
ACCESS_CONTROL_ALLOW_METHODS = 'HEAD,GET,POST,DELETE,PUT,OPTIONS'
DISABLE_CACHING = True
ALLOW_RANDOM_QUERY = True
