import requests
from scrapy.spiders import Spider
from scrapy_selenium.http import SeleniumRequest
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium.webdriver.common.by import By

from ..helper import JsonLdMixin


class DisProtSpider(Spider, JsonLdMixin):
    name = 'disprot'
    # DisProt has robots.txt and sitemap.xml
    # but they also have a publicly documented API
    # FIXME: use API and the sitemap, but sitemap is preferred when only one can be
    #  implemented. currently because SitemapSpider has to yield Request, we are
    #  forced to use the API only and rely on the page url pattern not changing

    # on the other hand, they use client side rendering, which is very annoying
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_selenium.SeleniumMiddleware': 800,
        }
    }

    def start_requests(self):
        ids = requests.get('https://disprot.org/api/list_ids').json()['disprot_ids']
        for disprot_id in ids:
            yield SeleniumRequest(
                url=f'http://disprot.org/{disprot_id}',
                callback=self.extract_jsonld,
                wait_time=10,
                wait_until=presence_of_element_located(
                    (By.XPATH,
                     '//script[@type="application/ld+json"]')
                )
            )
