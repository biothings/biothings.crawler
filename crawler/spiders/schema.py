# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class SchemaSpider(CrawlSpider):
    name = 'schema'
    allowed_domains = ['schema.org']
    start_urls = ['http://schema.org/']
    rules = (Rule(LinkExtractor(deny=r'\.json(ld)?'), callback='parse_page', follow=True),)

    def parse_page(self, response):

        for href in response.css('a::attr(href)').getall():
            if href.endswith('.jsonld'):
                yield {
                    "from": response.url,
                    "link": href
                }
