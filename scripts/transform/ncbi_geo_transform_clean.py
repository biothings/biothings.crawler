import asyncio
import logging

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Document
from elasticsearch_dsl.serializer import serializer
from web.index import transform

client = Elasticsearch('localhost:9200', serializer=serializer)


async def structure_ncbi_geo():
    global count
    search = Search(using=client, index='ncbi_geo_transformed')
    search = search.params(scroll='1d', sort=['_id'])
    for doc in search.scan():
        if 'funding' in doc:
            new_list = []
            for funding in doc.funding:
                if funding['funder']['name'] and len(funding['identifier']) > 3:
                    new_list.append(funding)
            logging.info("%s -> %s", len(doc.funding), len(new_list))
            if not new_list:
                new_list = None
            client.update(
                index='ncbi_geo_transformed',
                id=doc.meta.id,
                body={
                    "doc": {
                        "funding": new_list}})


logging.basicConfig(level='INFO')
loop = asyncio.get_event_loop()
loop.run_until_complete(structure_ncbi_geo())
