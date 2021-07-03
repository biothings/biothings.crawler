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
    immport_search_payload = {
        'pageSize': 1000,
        'conditionOrDisease': 'COVID-19 - DOID:0080600',
    }
