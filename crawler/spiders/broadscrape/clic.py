# -*- coding: utf-8 -*-
"""
    CLIC CTSA Program Hub
    https://clic-ctsa.org/hubs

    robots.txt as of 03/30/2020
    https://clic-ctsa.org/robots.txt

    User-agent: *
    # CSS, JS, Images in these path
    Allow: /core/*
    Allow: /profiles/*
    # Directories
    Disallow: /core/
    Disallow: /profiles/
    # Files
    Disallow: /README.txt
    Disallow: /web.config
    # Paths (clean URLs)
    Disallow: /admin/
    Disallow: /comment/reply/
    Disallow: /filter/tips
    Disallow: /node/add/
    Disallow: /search/
    Disallow: /user/*
    # Paths (no clean URLs)
    Disallow: /index.php/*

    The file content has been simplified.
"""

import glob
import logging
import logging.handlers
from urllib.parse import urlparse

from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, Spider

from ..helper import JsonLdMixin

LOG_FILENAME = 'clic.log'

# Set up a specific logger with our desired output level
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=1000000, backupCount=100)

logger.addHandler(handler)


class CLICSpider(Spider, JsonLdMixin):

    name = 'clic'
    start_urls = ["https://clic-ctsa.org/hubs"]
    
    custom_settings = {
        'DEPTH_LIMIT': 3,
        'SCHEDULER_PRIORITY_QUEUE': 'scrapy.pqueues.DownloaderAwarePriorityQueue',
        'LOG_LEVEL' : 'INFO',
        'COOKIES_ENABLED': False,
        'RETRY_ENABLED': False,
        'DOWNLOAD_TIMEOUT': 15,
        'REDIRECT_ENABLED': False,
        # https://docs.scrapy.org/en/latest/faq.html \
        # #does-scrapy-crawl-in-breadth-first-or-depth-first-order
        'DEPTH_PRIORITY': 1,
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue'
    }
    def start_requests(self):
        for url in self.start_urls:
            yield Request(url, callback=self.parse_start_url)

    def parse_start_url(self, response):
        path = ('//*[@id="content"]/section/div[2]/div[2]/div'
                '/div[2]/div/table/tbody/tr/td[1]/a/@href')
        hubs = response.xpath(path).getall()
        for hub_url in hubs:
            yield Request(response.urljoin(hub_url),
                          callback=self.parse_site_url)

    def parse_site_url(self, response):
        sections_path = '//*[@id="content"]/section/div/div[3]/div/div[1]'
        for section_selector in response.xpath(sections_path):
            if section_selector.xpath('text()').get() == 'Website':
                url = section_selector.xpath('following-sibling::div//a/@href').get()
                if url:
                    yield Request(url)

    def parse(self, response):
        logger.info(response.url)
        for item in self.extract_jsonld(response, response.url):
            yield item
        domain = urlparse(response.url).hostname or ''
        domain = domain.lstrip('www.')
        extractor = LinkExtractor(allow_domains=(domain,))
        for url in extractor.extract_links(response):
            yield response.follow(url)
