"""
    Reindex Script

    When items are first crawled, they are stored in ES with mapping disabled.
    So that field type conflicts would not interrupt the crawling process.

    To make the crawled items searchable and aggregatable, reindexing is needed.
    This script automates the process and handles mapping type conflicts.

"""


import logging
import os

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.serializer import serializer

SRC_ES_HOST = os.getenv('SRC_ES_HOST', 'localhost:9199')
DST_ES_HOST = os.getenv('DST_ES_HOST', 'localhost:9200')
SRC_ES_INDEX = os.environ['SRC_ES_INDEX']


def index(index_name):
    """
        Fault Tolerent Reindex

        Tries to index each item to the first destination index:
            - indexed_source
            - indexed_source_1
            - indexed_source_2
            - ...

        Unless reaching the maximum per source index limit.

    """

    src_client = Elasticsearch(SRC_ES_HOST, serializer=serializer)
    dst_client = Elasticsearch(DST_ES_HOST, serializer=serializer)

    search = Search(using=src_client, index=index_name)
    search = search.params(scroll='1d', sort=['_id'])

    new_index_name = f'indexed_{SRC_ES_INDEX}'

    for doc in search.scan():  # scroll over all documents

        suffix_number = 0  # tolerant mapping failure

        while suffix_number < 100:  # maximum index number for one source

            # no suffix for the first index
            suffix = f'_{suffix_number}' if suffix_number else ''

            try:
                dst_client.index(
                    index=new_index_name + suffix,
                    body=doc.to_dict(),
                    id=doc.meta.id)

            except Exception:
                suffix_number += 1

            else:
                break


if __name__ == "__main__":

    logging.basicConfig(level='INFO')
    index(SRC_ES_INDEX)
