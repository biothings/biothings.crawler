import imp
import urllib.request

from tornado.web import RequestHandler

try:
    file = urllib.request.urlopen('https://raw.githubusercontent.com/flaneuse/biodata-portal/master/discovery/web/siteconfig.py')
    my_code = file.read()
    mymodule = imp.new_module('mymodule')
    exec(my_code, mymodule.__dict__)
except Exception:
    pass

class MainHandler(RequestHandler):
    def get(self):
        if mymodule.REPOSITORIES:
            self.render('index.html',sources=mymodule.REPOSITORIES)
        else:
            self.render('index.html',sources='')
