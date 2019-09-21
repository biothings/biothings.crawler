# -*- coding: utf-8 -*-
import json
import logging
import os

import scrapy
from extruct.jsonld import JsonLdExtractor


class HarvardSpider(scrapy.Spider):

    name = 'harvard'
    base_url = 'https://dataverse.harvard.edu/api/search?q=*&type=dataset'
    start_urls = [
        base_url
    ]

    def parse(self, response, start=79320):

        api_res = json.loads(response.body)
        assert api_res['status'] == 'OK'

        for item in api_res['data']['items']:
            if os.path.exists(self.get_filename(item['global_id'])):
                logging.info('%s already downloaded.', item['global_id'])
            else:
                yield scrapy.Request(
                    url=item['url'],
                    callback=self.parse_jsonld,
                    cb_kwargs={
                        'global_id': item['global_id']
                    }
                )

        logging.info('Requesting next page from %s.', start + 10)
        if len(api_res['data']['items']) == 10:
            yield scrapy.Request(
                url=self.base_url + '&start=' + str(start + 10),
                cb_kwargs={
                    'start': start + 10
                }
            )
        else:
            logging.info('No more data to crawl. Current start: %s.', start)

    def parse_jsonld(self, response, global_id):

        data = JsonLdExtractor().extract(response.body)
        if len(data) == 0:
            logging.info('No json-ld found in %s.', response.url)
        elif len(data) == 1:
            logging.info('found jsonld')
            if data[0].get('@type', '').endswith("Dataset"):
                with open(self.get_filename(global_id), 'w') as f:
                    json.dump(data, f, indent=2)
                    logging.info('SUCCESS')
            else:
                logging.warning('not dataset type')
        else:
            logging.warning('more than 1 jsonld')

    def get_filename(self, global_id):

        return global_id.replace('/', '_').replace(':', '_') + '.json'
