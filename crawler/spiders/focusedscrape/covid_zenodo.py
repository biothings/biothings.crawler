
# https://github.com/SuLab/outbreak.info-resources/issues/10#issuecomment-620188085

import json
import logging

from scrapy import Request, Spider

from ..helper import JsonLdMixin


class ZenodoCovidSpider(Spider, JsonLdMixin):

    name = 'zenodo_covid'
    start_urls = ['https://zenodo.org/api/records/?page=1&size=1000&communities=covid-19']

    def parse(self, response):

        response = json.loads(response.body)
        hits = response['hits']['hits']
        total = len(hits)

        for index, hit in enumerate(hits):

            logging.info('[%s/%s] %s', index + 1, total, hit.get('doi'))

            try:
                url = hit['links']['doi']

            except KeyError:
                yield {
                    "_type": "error",
                    "_document": hit
                }
            else:
                yield Request(
                    url=url,
                    callback=self.extract_jsonld,
                    cb_kwargs={
                        '_id': url
                    }
                )