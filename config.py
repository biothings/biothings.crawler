
from biothings.web.settings.default import *
from biothings.web.api.es.handlers import QueryHandler

# *****************************************************************************
# Elasticsearch
# *****************************************************************************
ES_INDEX = '_all'
ES_DOC_TYPE = '_doc'
ES_HOST = ['localhost:9199']

# *****************************************************************************
# Tornado URL Patterns
# *****************************************************************************
UNINITIALIZED_APP_LIST = [
    # (r"/sitemap.xml", RedirectHandler, {"url": "/static/sitemap.xml"}),
]
API_ENDPOINTS = [
    (r"/api/query/?", QueryHandler),
]
APP_LIST = API_ENDPOINTS  # + WEB_ENDPOINTS

# *****************************************************************************
# Biothings SDK Settings
# *****************************************************************************
ACCESS_CONTROL_ALLOW_METHODS = 'HEAD,GET,POST,DELETE,PUT,OPTIONS'
DISABLE_CACHING = True
