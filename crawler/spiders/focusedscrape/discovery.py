# -*- coding: utf-8 -*-
'''
    CTSA Data Discovery Engine Spider

    Scrape with public api access
    https://discovery.biothings.io/api/dataset

'''

import json
import scrapy

API_URL = 'https://discovery.biothings.io/api/dataset'
DOC_BASE = 'https://discovery.biothings.io/dataset/'


class DiscoverySpider(scrapy.Spider):
    """
    Crawl CTSA Data Discovery Engine

    Understand that an item with _id 83dc3401f86819de is on:
    https://discovery.biothings.io/dataset/83dc3401f86819de

    """

    name = 'discovery'
    start_urls = [API_URL]

    def parse(self, response):

        body = json.loads(response.body)

        for doc in body['hits']:
            doc['_id'] = DOC_BASE + doc['_id']
            yield doc


def main():
    """
    Crawl CTSA Data Discovery Engine.
    You can run this file directly.
    """

    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    process = CrawlerProcess(get_project_settings())
    process.crawl(DiscoverySpider)
    process.start()


if __name__ == '__main__':

    main()
