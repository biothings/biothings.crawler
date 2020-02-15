# -*- coding: utf-8 -*-
"""
    Zenodo Datasource

    No robots.txt as of 02/15/2020

"""

from scrapy.spiders import SitemapSpider

from ..helper import JsonLdMixin


class ZenodoSpider(SitemapSpider, JsonLdMixin):

    name = 'zenodo'
    sitemap_urls = ['http://www.zenodo.org/sitemap.xml']
    sitemap_rules = [('/record/', 'extract_jsonld')]
