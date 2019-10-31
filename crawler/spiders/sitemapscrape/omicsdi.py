# -*- coding: utf-8 -*-

from scrapy.spiders import SitemapSpider

from ..helper import JsonLdMixin


class OmicsdiSpider(SitemapSpider, JsonLdMixin):

    name = 'omicsdi'

    sitemap_urls = ['http://www.omicsdi.org/sitemap.xml']
    sitemap_rules = [('/dataset/', 'extract_jsonld')]

    def extract_jsonld(self, response):
        for jsld in super().extract_jsonld(response, response.url):
            yield jsld.get('mainEntity')
