# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

"""
Elasticsearch Backend for Scrapy Spider.

* Recommend setting up index mapping ahead of time.
* Use Env ES_HOST to override default es address.
* Use Env ES_INDEX to override spider name as index.
* Require Elasticsearch version ~6.5+.
* Assume default doc_type or no doc_type.

"""
import logging
import os

from elasticsearch import Elasticsearch

from crawler.upload import CrawlerIndices

indices = CrawlerIndices(Elasticsearch(os.getenv('ES_HOST')))


class ESPipeline:
    """
    Store received items in Elasticsearch.
    Spider name will be used as the index name.
    """

    def process_item(self, item, spider):
        """
        Make Elasticsearch index request.

        * Use spider-injected _id field if possible
          otherwise use @id field in the document
          generate random id if none of them exist

        """

        _id = item.pop('_id', item.get('@id'))

        # FIXME: ES only allows lowercase
        _index = os.getenv('ES_INDEX', 'crawler_' + spider.name)

        res = indices.index(_index, _id, item)
        logging.debug(res)

        return item
