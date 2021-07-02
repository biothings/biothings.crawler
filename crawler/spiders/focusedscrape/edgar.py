from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

from scrapy.http import Request

from ..helper import JsonLdMixin


class EdgarSpider(CrawlSpider, JsonLdMixin):
    name = 'eDGAR'
    allowed_domains = ['edgar.biocomp.unibo.it']
    start_urls = [
        'http://edgar.biocomp.unibo.it/cgi-bin/gene_disease_db/main_table.py',
    ]

    rules = (
        Rule(LinkExtractor(allow=r'gene.py\?gene=.+'), callback='extract_jsonld'),
    )
