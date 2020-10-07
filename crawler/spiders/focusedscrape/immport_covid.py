#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""ImmPort Spider for COVID related studies only

Based on the generic ImmPort spider.

To run this spider:
    $ scrapy crawl immport_covid

"""

import scrapy
import requests

from .immport import ImmPortSpider


class ImmPortCovidSpider(ImmPortSpider):
    # override so we are only looking at covid results
    name = 'immport_covid'

    def start_requests(self):
        # they have less than 1000 studies in total
        # so don't need to worry about pagination for now.
        payload = {'pageSize': 1000,
                   'conditionOrDisease': 'COVID-19 - DOID:0080600'}
        base_url = "https://www.immport.org/shared/data/query/search?term="
        try:
            j = requests.get(base_url, params=payload, timeout=5).json()
            ids = [h['_id'] for h in j['hits']['hits']]
        except requests.exceptions.Timeout:
            # catch the rather harmless exception, and do nothing
            ids = []
            self.logger.warning("Timeout searching COVID IDs")

        prefix = "https://www.immport.org/shared/study/"
        for id_ in ids:
            yield scrapy.Request(prefix + id_)
