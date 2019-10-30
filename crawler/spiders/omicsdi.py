# -*- coding: utf-8 -*-
import json
import logging
import os

import scrapy
from extruct.jsonld import JsonLdExtractor
from scrapy.spiders import SitemapSpider


# create logger
no_json_ld = logging.getLogger('omicsdi.no_json_ld')
not_dataset = logging.getLogger('omicsdi.not_dataset')
no_json_ld.setLevel(logging.DEBUG)
not_dataset.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh1 = logging.FileHandler('omicsdi_no_jsonld.log')
fh2 = logging.FileHandler('omicsdi_not_dataset.log')
fh1.setLevel(logging.DEBUG)
fh2.setLevel(logging.DEBUG)
# add the handlers to the logger
no_json_ld.addHandler(fh1)
not_dataset.addHandler(fh2)


class OmicsdiSpider(SitemapSpider):

    name = 'omicsdi'

    sitemap_urls = ['http://www.omicsdi.org/sitemap.xml']
    sitemap_rules = [('/dataset/', 'parse_ds')]

    def parse_ds(self, response):

        jslds = JsonLdExtractor().extract(response.body)

        if not jslds:

            no_json_ld.info(response.url)

        else:

            for jsld in jslds:

                if jsld.get('@type', '').endswith("Dataset"):

                    jsld['_id'] = response.url
                    yield jsld

                else:

                    found_at_least_one = False

                    for key in jsld:

                        if isinstance(jsld[key], dict):

                            if jsld[key].get('@type', '').endswith("Dataset"):

                                found_at_least_one = True
                                jsld[key]['_id'] = response.url
                                yield jsld[key]
                    
                    if not found_at_least_one:

                        not_dataset.info(response.url)
