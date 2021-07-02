from scrapy.spiders import SitemapSpider

from ..helper import JsonLdMixin


class MassBankSpider(SitemapSpider, JsonLdMixin):
    name = 'massbank'
    # have sitemap at https://massbank.eu/MassBank/sitemapindex.xml
    # prefer robots.txt
    sitemap_urls = ['https://massbank.eu/robots.txt']
    sitemap_rules = [
        (r'/MassBank/RecordDisplay\?id=.+', 'extract_jsonld')
    ]
