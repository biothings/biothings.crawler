# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import logging

from elasticsearch import Elasticsearch
from scrapy.exceptions import DropItem

# es = Elasticsearch('localhost:9200')
es = Elasticsearch('localhost:9199')

# class DiscoveryPipeline(object):
#     def process_item(self, item, spider):
#         return item


class ESPipeline(object):

    def process_item(self, item, spider):

        res = es.index(index=spider.name, id=item.pop('_id'), body=item)
        logging.debug(res)

        return item
