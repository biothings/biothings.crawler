# -*- coding: utf-8 -*-
from scrapy.spiders import SitemapSpider

from ..helper import JsonLdMixin


class ZenodoSpider(SitemapSpider, JsonLdMixin):

    name = 'zenodo'
    sitemap_urls = ['http://www.zenodo.org/sitemap.xml']
    sitemap_rules = [('/record/', 'extract_jsonld')]
