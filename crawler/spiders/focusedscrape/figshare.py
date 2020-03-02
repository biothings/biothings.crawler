# -*- coding: utf-8 -*-
"""
    Figshare

    robots.txt as of 02/15/2020
    https://figshare.com/robots.txt

        User-agent: *
        Disallow: /search
        Sitemap: https://figshare.com/sitemap/siteindex.xml

    Required to configure es:

    PUT figshare_api
    {
        "mappings": {
            "enabled": true,
            "dynamic_templates": [
                {
                    "published_date_fild": {
                        "match_mapping_type": "date",
                        "mapping": {
                            "type": "date"
                        }
                    }
                },
                {
                    "object_fields": {
                        "match_mapping_type": "object",
                        "mapping": {
                            "enabled": false
                        }
                    }
                },
                {
                    "all_other_fields": {
                        "match_mapping_type": "*",
                        "mapping": {
                            "index": false
                        }
                    }
                }
            ]
        }
    }

"""
import json
import logging
import os

import scrapy
from elasticsearch import Elasticsearch

from ..helper import JsonLdMixin


class FigshareAPISpider(scrapy.Spider):
    """

    May need to run multiple times

    Example field:
    "published_date": "1996-01-10T00:00:00Z"
    """

    name = 'figshare_api'
    base_url = 'https://api.figshare.com/v2/articles?'
    client = Elasticsearch(os.getenv('ES_HOST', 'localhost:9200'))
    query_params = {
        "page_size": "1000",
        "order": "published_date",
        "order_direction": "asc",
        "item_type": "3"  # dataset type
    }

    def form_url(self, **kwargs):

        params = ['='.join(item_pair) for item_pair in self.query_params.items()]
        url = self.base_url + '&'.join(params)
        for key, value in kwargs.items():
            if value:
                url += f'&{key}={value}'
        return url

    def start_requests(self):

        res = self.client.indices.get_mapping(index=self.name)
        mapping = res[self.name]['mappings']
        published_date = mapping.get('_meta', {}).get('published_date', '')
        url = self.form_url(published_since=published_date)
        return [scrapy.FormRequest(url, callback=self.parse, cb_kwargs={'published_since': published_date})]

    def parse(self, response, published_since='', page=1):

        api_res = json.loads(response.body)
        published_date = published_since

        if isinstance(api_res, list):

            for item in api_res:
                published_date = item['published_date'][:10]
                # skip already scrapped
                if self.client.exists(index=self.name, id=item['url']):
                    logging.info('Skipping %s.', item['url'])
                    continue
                item.update(_id=item['id'])
                yield item

            self.client.indices.put_mapping(index=self.name, body={"_meta": {"published_date": published_date}})
            logging.info('Requesting page %s since %s.', page + 1, published_since)

            yield scrapy.Request(
                url=self.form_url(page=page + 1, published_since=published_since),
                cb_kwargs={
                    'page': page + 1
                }
            )

        else:

            logging.info('Ending on page %s for %s.', page, api_res)
