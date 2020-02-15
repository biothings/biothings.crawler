# -*- coding: utf-8 -*-
'''
    NCBI Geo Datasource

    Scrape contents in HTML tables
    Not structured metadata

    https://www.ncbi.nlm.nih.gov/robots.txt
    Declared No Robots and crawl delay as of 02/15/2020.
    Defined long rules so the file content is not included here.

'''

import scrapy


class NCBIGeoSpider(scrapy.Spider):

    name = 'ncbi_geo'

    def start_requests(self):

        start = 1
        end = 137890
        prefix = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE"
        for acc_id in range(start, end + 1):
            yield scrapy.Request(url=prefix + str(acc_id))

    def parse(self, response):

        table = response.xpath(
            '/html/body/table/tr/td/table[6]/tr[3]/td[2]'
            '/table/tr/td/table/tr/td/table[2]/tr/td'
            '/table[1]/tr')
        data = {}

        for node in table:
            # extract series id
            if node.attrib.get('bgcolor') == '#cccccc':
                data['_id'] = node.xpath('.//strong').attrib.get('id')
            # remove place holder lines
            elif len(node.xpath('./td')) == 2:
                if node.xpath('string(./td[1])').get().strip():
                    # extract multi item entry
                    if node.xpath('./td[2]').attrib.get('onmouseout'):
                        key = node.xpath('./td[1]/text()').get().split()[0]
                        data[key] = node.xpath('./td[2]//a/text()').getall()
                    # extract single item entry
                    else:
                        key = node.xpath('./td[1]/text()').get()
                        data[key] = node.xpath('string(./td[2])').get().strip().replace('\xa0', ' ')

        return data if data else None


def main():
    """
    Crawl NCBI Geo Datasets.
    You can run this file directly.
    """

    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    process = CrawlerProcess(get_project_settings())
    process.crawl(NCBIGeoSpider)
    process.start()


if __name__ == '__main__':

    main()
