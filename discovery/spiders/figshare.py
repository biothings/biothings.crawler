# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class FigshareSpider(CrawlSpider):
    name = 'figshare'
    allowed_domains = ['figshare.com']
    start_urls = ['https://brunel.figshare.com/articles/Enhanced_simple_beam_theory_for_characterising_mode-I_fracture_resistance_via_a_double_cantilever_beam_test/7409669']
    rules = (Rule(LinkExtractor(deny=r'\.json(ld)?'), callback='parse_page', follow=True),)

    def parse_page(self, response):

        for href in response.css('a::attr(href)').getall():
            if href.endswith('.jsonld') or href.endswith('.json'):
                yield {
                    "from": response.url,
                    "link": href
                }
