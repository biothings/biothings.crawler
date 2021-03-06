# -*- coding: utf-8 -*-
"""
    Figshare Brunel

    robots.txt as of 02/15/2020
    https://figshare.com/robots.txt

        User-agent: *
        Disallow: /search
        Sitemap: https://figshare.com/sitemap/siteindex.xml

"""

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from ..helper import JsonLdMixin


class FigshareBrunelSpider(CrawlSpider, JsonLdMixin):

    name = 'figshare_brunel'
    allowed_domains = ['figshare.com']
    start_urls = [('https://brunel.figshare.com/browse')]
    rules = (
        Rule(LinkExtractor(allow=r'/articles/'), callback='extract_jsonld'),
        Rule(LinkExtractor())
    )
