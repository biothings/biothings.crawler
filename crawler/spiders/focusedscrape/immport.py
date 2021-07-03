# -*- coding: utf-8 -*-
'''
    ImmPort Spider
    Scrape shared studies on ImmPort

    Entry Point: https://www.immport.org/shared/search
    Example Pages:
        Without Pmid: https://www.immport.org/shared/study/SDY1
        With Pmid: https://www.immport.org/shared/study/SDY1025

    No robots.txt detected.

    More on https://github.com/biothings/biothings.crawler/issues/1
'''

import requests
import scrapy
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.support.expected_conditions import title_is

from ..helper import JsonLdMixin


class ImmPortSpider(scrapy.Spider, JsonLdMixin):
    """
    Crawl ImmPort with Selenium

    * User Selenium Chrome Driver to render dynamic contents
    * Scrape HTML table elements to JSON dictionary
    * Use mannually downloaded data ID file

    """

    name = 'immport'
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_selenium.SeleniumMiddleware': 800,
        }
    }
    immport_search_payload = {'pageSize': 1000}


    def start_requests(self):
        base_url = "https://www.immport.org/shared/data/query/search?term="
        try:
            j = requests.get(base_url, timeout=5,
                             params=self.immport_search_payload,).json()
            ids = [h['_id'] for h in j['hits']['hits']]
        except requests.exceptions.Timeout:
            # catch the rather harmless exception, and do nothing
            ids = []
            self.logger.warning("Timeout searching ImmPort IDs")

        prefix = "https://www.immport.org/shared/study/"
        for id_ in ids:
            # FIXME: the explicit wait condition can break any day
            #  but on the bright side, it only makes crawling extremely slow.
            #  If you're run into issues into future, adjust the wait conditions
            yield SeleniumRequest(
                url=prefix + id_,
                callback=self.extract_jsonld,
                wait_time=10,
                wait_until=title_is("Study Detail")
            )

    def parse(self, response, **kwargs):
        return None


def main():
    """
    Crawl ImmPort.
    You can run this file directly.
    """

    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    process = CrawlerProcess(get_project_settings())
    process.crawl(ImmPortSpider)
    process.start()


if __name__ == '__main__':
    main()
