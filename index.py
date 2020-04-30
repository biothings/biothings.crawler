'''
    Biothings Crawler API Server
'''

from biothings.web.index_base import main
from web.handlers import MainHandler


if __name__ == '__main__':
    main([
    (r'/', MainHandler),
])
