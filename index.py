
from biothings.web.index_base import main, options
from biothings.web.settings import BiothingESWebSettings


web_settings = BiothingESWebSettings(config='config')

APP_LIST = web_settings.generate_app_list()

if __name__ == '__main__':
    main(APP_LIST)
