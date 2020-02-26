'''
    Biothings Crawler API Server
'''
import os

from biothings.web.settings import BiothingESWebSettings
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado.web import Application, StaticFileHandler, RedirectHandler, RequestHandler

import urllib.request
import imp

file = urllib.request.urlopen('https://raw.githubusercontent.com/flaneuse/biodata-portal/master/discovery/web/siteconfig.py')
my_code = file.read()
mymodule = imp.new_module('mymodule')
exec(my_code, mymodule.__dict__)

define("port", default="8000", help="local port to run the server")
options.parse_command_line()


public_root = os.path.join(os.path.dirname(__file__), 'static')

class MainHandler(RequestHandler):
    def get(self):
        if mymodule.REPOSITORIES:
            self.render('index.html',sources=mymodule.REPOSITORIES)
        else:
            self.render('index.html',sources='')

API_SETTINGS = BiothingESWebSettings(config='web.config')
APP_LIST = API_SETTINGS.generate_app_list() + [
    (r'/', MainHandler),
    (r'/(.*)', StaticFileHandler, {'path': public_root}),
]

settings = dict(
  debug=True,
  static_path=public_root,
  template_path=public_root
)


def main():

    application = Application(APP_LIST, **settings)

    server = HTTPServer(application)
    server.bind(options.port)
    server.start()

    IOLoop.current().start()


if __name__ == '__main__':
    main()
