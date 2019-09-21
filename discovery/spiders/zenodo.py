# -*- coding: utf-8 -*-
import json
import logging
import os

import scrapy
from extruct.jsonld import JsonLdExtractor
from scrapy.spiders import SitemapSpider


# create logger
no_json_ld = logging.getLogger('zenodo.no_json_ld')
not_dataset = logging.getLogger('zenodo.not_dataset')
no_json_ld.setLevel(logging.DEBUG)
not_dataset.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh1 = logging.FileHandler('zenodo_no_jsonld.log')
fh2 = logging.FileHandler('zenodo_not_dataset.log')
fh1.setLevel(logging.DEBUG)
fh2.setLevel(logging.DEBUG)
# add the handlers to the logger
no_json_ld.addHandler(fh1)
not_dataset.addHandler(fh2)


class ZenodoSpider(SitemapSpider):

    name = 'zenodo'

    sitemap_urls = ['http://www.zenodo.org/sitemap.xml']
    sitemap_rules = [('/record/', 'parse_record')]

    def parse_record(self, response):

        jslds = JsonLdExtractor().extract(response.body)

        if not jslds:

            no_json_ld.info(response.url)

        else:

            for jsld in jslds:

                if jsld.get('@type', '').endswith("Dataset"):

                    yield jsld

                else:
                    not_dataset.info(response.url)
