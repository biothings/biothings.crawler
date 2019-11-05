import asyncio
import logging

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Document
from elasticsearch_dsl.serializer import serializer
from web.index import transform

client = Elasticsearch('localhost:9199', serializer=serializer)


def index(index_name):
    search = Search(using=client, index=index_name)
    search = search.params(scroll='1d', sort=['_id'])
    new_index_prefix = index_name + '_search_'
    for doc in search.scan():
        count = 0
        while True:
            try:
                client.index(
                    index=new_index_prefix + str(count),
                    body=doc.to_dict(),
                    id=doc.meta.id)
            except Exception:
                count += 1
            else:
                break


logging.basicConfig(level='INFO')
index('ncbi_geo_transformed')
index('omicsdi')
