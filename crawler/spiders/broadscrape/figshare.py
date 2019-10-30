# -*- coding: utf-8 -*-

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from .. import JsonLdMixin


class FigshareSpider(CrawlSpider, JsonLdMixin):

    name = 'figshare'
    allowed_domains = ['figshare.com']
    start_urls = [('https://brunel.figshare.com/browse')]
    rules = (
        Rule(LinkExtractor(allow=r'/articles/'), callback='extract_jsonld'),
        Rule(LinkExtractor())
    )
