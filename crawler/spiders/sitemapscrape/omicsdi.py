# -*- coding: utf-8 -*-
"""
    Omicsdi Datasource

    robots.txt as of 2/15/2020
    https://www.omicsdi.org/robots.txt

        User-agent: *
        Allow: /
        Disallow: /ws
        Disallow: /ws/dataset/search
        Allow: /ws/dataset
        Allow: /ws/term
        Allow: /ws/statistics

        Sitemap: https://www.omicsdi.org/sitemap.xml

"""
from scrapy.spiders import SitemapSpider

from ..helper import JsonLdMixin


class OmicsdiSpider(SitemapSpider, JsonLdMixin):

    name = 'omicsdi'

    sitemap_urls = ['http://www.omicsdi.org/sitemap.xml']
    sitemap_rules = [('/dataset/', 'extract_jsonld')]

    def extract_jsonld(self, response):
        for jsld in super().extract_jsonld(response, response.url):
            yield jsld.get('mainEntity')
