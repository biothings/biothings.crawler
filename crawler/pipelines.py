# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import logging

from elasticsearch import Elasticsearch
from scrapy.exceptions import DropItem

es = Elasticsearch('localhost:9200')


class ESPipeline(object):

    def process_item(self, item, spider):

        _id = item.pop('_id', item.get('@id'))
        # use spider-injected _id field if possible
        # otherwise use @id field in the document
        # generate random id if none of them exist
        res = es.index(index=spider.name, id=_id, body=item)
        logging.debug(res)

        return item
