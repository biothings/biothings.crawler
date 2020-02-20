'''
    Biothings Crawler API Server
'''

from biothings.web.settings import BiothingESWebSettings
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado.web import Application, StaticFileHandler, RedirectHandler


define("port", default="8000", help="local port to run the server")
options.parse_command_line()


API_SETTINGS = BiothingESWebSettings(config='web.config')
APP_LIST = API_SETTINGS.generate_app_list() + [
    (r"/", RedirectHandler, dict(url='/index.html')),
    (r"/(index.html)", StaticFileHandler, dict(path='static')),
]


def main():

    application = Application(APP_LIST)

    server = HTTPServer(application)
    server.bind(options.port)
    server.start()

    IOLoop.current().start()


if __name__ == '__main__':
    main()
