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


class ESClient:

    def __init__(self):
        self.host = os.getenv('ES_HOST')
        self.client = Elasticsearch(self.host)
        self.version = int(self.client.info()['version']['number'].split('.')[0])
        self._valid_indices = set()

    def index(self, _index, _id, _source):

        if _index not in self._valid_indices:
            if not self.client.indices.exists(index=_index):
                request_body = {
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    },
                    "mappings": {
                        "enabled": "false"
                    }
                }
                if self.version < 7:
                    request_body['mappings'] = {
                        "_doc": request_body['mappings']
                    }
                self.client.indices.create(
                    index=_index,
                    body=request_body)
            self._valid_indices.add(_index)

        return self.client.index(index=_index, id=_id, body=_source)


client = ESClient()


class ESPipeline:
    """
    Store received items in Elasticsearch.
    Spider name will be used as the index name.
    """

    def __init__(self):
        self.client = client

    def process_item(self, item, spider):
        """
        Make Elasticsearch index request.

        * Use spider-injected _id field if possible
          otherwise use @id field in the document
          generate random id if none of them exist

        """

        _id = item.pop('_id', item.get('@id'))
        _index = os.getenv('ES_INDEX', 'crawler_' + spider.name)

        res = client.index(_index, _id, item)
        logging.debug(res)

        return item
