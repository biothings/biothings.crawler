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
import os

import elasticsearch
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

    "createdAt": "2011-02-20T14:25:02Z" is used for sorting by date
    """

    name = 'harvard_tracing'
    base_url = 'https://dataverse.harvard.edu/api/search?q=*&type=dataset&sort=date&per_page=500&order=asc'
    client = elasticsearch.Elasticsearch(os.getenv('ES_HOST', 'localhost:9200'))
    step = 500

    def start_requests(self):

        res = self.client.indices.get_mapping(index=self.name)
        mapping = res[self.name]['mappings']
        next_start = mapping.get('_meta', {}).get('next_start', 0)
        url = self.base_url + '&start=' + str(next_start)
        return [scrapy.FormRequest(url, callback=self.parse, cb_kwargs={'start': next_start})]

    def parse(self, response, start=0):

        api_res = json.loads(response.body)
        assert api_res['status'] == 'OK'
        next_start = start + self.step

        for item in api_res['data']['items']:
            # skip already scrapped
            if self.client.exists(index=self.name, id=item['url']):
                logging.info('Skipping %s.', item['url'])
                continue
            try:
                r = requests.head(item['url'], allow_redirects=True)
                yield {
                    "_id": item['url'],
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
                    "_id": item['url'],
                    "success": False,
                    "exception": str(Exception)
                }

        logging.info('Requesting next page from item %s.', next_start)
        self.client.indices.put_mapping(index=self.name, body={"_meta":{"next_start":next_start}})

        if api_res['data']['start'] <= api_res['data']['total_count']:
            yield scrapy.Request(
                url=self.base_url + '&start=' + str(next_start),
                cb_kwargs={
                    'start': next_start
                }
            )
        else:
            logging.info('No more data to crawl. Current start: %s.', start)
