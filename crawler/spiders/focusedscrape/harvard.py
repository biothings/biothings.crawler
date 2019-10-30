# -*- coding: utf-8 -*-
import json
import logging

import scrapy
from .. import JsonLdMixin


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
                    '_id': item['global_id']
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
