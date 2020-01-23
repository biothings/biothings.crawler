""" Scrapy Spider Mixins """

import logging

from extruct.jsonld import JsonLdExtractor


class JsonLdMixin:
    """
    Scrapy Spider Mixin Class to Extract JSONLD.

    Example:

    class JSONLDSpider(scrapy.Spider, JsonLdMixin):

        name = 'SPIDER_NAME'

        def start_requests(self):

            urls = []
            for url in urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.extract_jsonld, # optional
                    cb_kwargs={
                        '_id': uuid.uuid1() # optional
                    })
    """

    def extract_jsonld(self, response, _id=None):
        """
        Scrapy Spider Request Callback Function

        * Inject an _id field for database pipeline
        * Use response URL as default _id

        """

        jslds = JsonLdExtractor().extract(response.body)

        for jsld in jslds:
            if _id:
                jsld['_id'] = _id
            else:
                jsld['_id'] = response.url

            logging.debug(jsld)
            yield jsld

    parse = extract_jsonld  # Set as the default parsing method
