
from biothings.web.index_base import main, options
from biothings.web.settings import BiothingESWebSettings
from tornado.options import define, options

define("redirect", default=8080, help="port to load resources")

web_settings = BiothingESWebSettings(config='config')

APP_LIST = web_settings.generate_app_list(reverse=True)

if __name__ == '__main__':
    main(APP_LIST)
