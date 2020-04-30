'''
    Biothings Crawler API Server
'''

from biothings.web.index_base import main
from tornado.web import RedirectHandler, RequestHandler, StaticFileHandler
from web.handlers import MainHandler


# settings = dict(
#   debug=True,
#   static_path=public_root,
#   template_path=public_root
# )




if __name__ == '__main__':
    main([
    (r'/', MainHandler),
    # (r'/(.*)', StaticFileHandler, {'path': public_root}),
])
