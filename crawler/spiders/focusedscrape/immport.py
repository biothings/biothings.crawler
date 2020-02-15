# -*- coding: utf-8 -*-
'''
    ImmPort Spider
    Scrape shared studies on ImmPort

    Entry Point: https://www.immport.org/shared/search
    Example Pages:
        Without Pmid: https://www.immport.org/shared/study/SDY1
        With Pmid: https://www.immport.org/shared/study/SDY1025

    No robots.txt detected.

    More on https://github.com/biothings/biothings.crawler/issues/1
'''

import csv

import scrapy


class ImmPortSpider(scrapy.Spider):
    """
    Crawl ImmPort with Selenium

    * User Selenium Chrome Driver to render dynamic contents
    * Scrape HTML table elements to JSON dictionary
    * Use mannually downloaded data ID file

    """

    name = 'immport_test'
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'crawler.middlewares.SeleniumChromeDownloaderMiddleware': 543,
        }
    }

    def start_requests(self):

        # assume ImmPort.txt is in the same folder
        with open(__file__[:-3] + '.txt', 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            ids = [row[0] for row in reader][1:]

        prefix = "https://www.immport.org/shared/study/"
        for id_ in ids:
            yield scrapy.Request(prefix + id_)

    def parse(self, response):

        data = {'_id': response.url}
        sections = response.xpath('//*[@id="ui-tabpanel-0"]/p-accordion/div/p-accordiontab')

        for section in sections:

            title = section.xpath('./div[1]//span/text()').get()

            if 'Summary' in title:
                rows = section.xpath('./div[2]/div/table/tbody/tr')
                for row in rows:
                    key = row.xpath('./td[1]/text()').get()
                    if key == 'Download Packages':
                        value = row.xpath('./td[2]/a').attrib.get('href')
                    else:
                        value = row.xpath('string(./td[2])').get()
                    data[key] = value

            elif 'Publications' in title:
                pmids = section.xpath(
                    './div[2]/div/p-table/div/div/table/tbody/tr/td[1]/a/text()').getall()
                if pmids:
                    data['Pubmed Id'] = pmids

            elif 'Study Links' in title:
                pass

            elif 'Glossary' in title:
                pass

        data = {key: value for key, value in data.items() if value}
        return data or None


def main():
    """
    Crawl ImmPort.
    You can run this file directly.
    """

    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    process = CrawlerProcess(get_project_settings())
    process.crawl(ImmPortSpider)
    process.start()


if __name__ == '__main__':

    main()
