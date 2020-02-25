# -*- coding: utf-8 -*-
"""
    Harvard Dataverse

    https://dataverse.harvard.edu/robots.txt
    Declared crawling delay as of 02/15/2020

        User-agent: *
        Disallow: /
        User-agent: Googlebot
        Allow: /$
        Allow: /dataset.xhtml
        Allow: /dataverse/
        Allow: /sitemap/
        Allow: /api/datasets/:persistentId/thumbnail
        Allow: /javax.faces.resource/images/
        Disallow: /dataverse/*?q
        Disallow: /dataverse/*/search
        Disallow: /
        Crawl-delay: 10
        # sitemap: https://dataverse.harvard.edu/sitemap/sitemap.xml
        # Created initially using: http://www.mcanerin.com/EN/search-engine/robots-txt.asp
        # Verified using: http://tool.motoricerca.info/robots-checker.phtml

"""
import json
import logging

import requests
import scrapy

from ..helper import JsonLdMixin


class HarvardSpider(scrapy.Spider, JsonLdMixin):

    name = 'harvard'
    base_url = 'https://dataverse.harvard.edu/api/search?q=*&type=dataset'
    start_urls = [
        base_url
    ]

    def parse(self, response, start=0):

        api_res = json.loads(response.body)
        assert api_res['status'] == 'OK'

        for item in api_res['data']['items']:

            yield scrapy.Request(
                url=item['url'],
                callback=self.extract_jsonld,
                cb_kwargs={
                    '_id': item['url']
                }
            )

        logging.info('Requesting next page from item %s.', start + 10)

        if len(api_res['data']['items']) == 10:
            yield scrapy.Request(
                url=self.base_url + '&start=' + str(start + 10),
                cb_kwargs={
                    'start': start + 10
                }
            )
        else:
            logging.info('No more data to crawl. Current start: %s.', start)


class HarvardTracingSpider(scrapy.Spider):
    """
    Resolve Redirections and Validate Links
    """

    name = 'harvard_tracing'
    base_url = 'https://dataverse.harvard.edu/api/search?q=*&type=dataset'
    start_urls = [
        base_url
    ]

    def parse(self, response, start=0):

        api_res = json.loads(response.body)
        assert api_res['status'] == 'OK'

        base = 0
        for item in api_res['data']['items']:

            base += 1
            try:
                r = requests.head(item['url'], allow_redirects=True)
                yield {
                    "_id": start + base,
                    "origin": item['url'],
                    "success": True,
                    "location": r.url,
                    "status": r.status_code,
                    "history": [{
                        "url": item.url,
                        "status": item.status_code
                    } for item in r.history]
                }
            except Exception:
                yield {
                    "_id": start + base,
                    "origin": item['url'],
                    "success": False,
                    "exception": str(Exception)
                }

        logging.info('Requesting next page from item %s.', start + 10)

        if len(api_res['data']['items']) == 10:
            yield scrapy.Request(
                url=self.base_url + '&start=' + str(start + 10),
                cb_kwargs={
                    'start': start + 10
                }
            )
        else:
            logging.info('No more data to crawl. Current start: %s.', start)
