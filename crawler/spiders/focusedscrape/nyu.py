# -*- coding: utf-8 -*-
"""
NYU Data Catalog

Example URL:
    https://datacatalog.med.nyu.edu/dataset/10042

ID Range:
    10001 - 10387 as of 12/12/2019

Metadata:
    Structured in schema.org standard.
    Statically embedded in dataset pages.
    Embedded as "application/ld+json" script tags.
"""

import scrapy

from ..helper import JsonLdMixin


class NYUDataCatalogSpider(JsonLdMixin, scrapy.Spider):
    """
    NYU Data Catalog Scrapy Spider

    * Crawling with known URL structure
    * Iterating through known ID range

    """

    name = 'nyu'

    def start_requests(self):

        start = 10001
        end = 10387
        prefix = "https://datacatalog.med.nyu.edu/dataset/"

        for _id in range(start, end + 1):

            url = prefix + str(_id)
            yield scrapy.Request(url)
