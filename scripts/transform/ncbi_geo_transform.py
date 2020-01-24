import asyncio
import logging

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from web.index import transform

client = Elasticsearch('localhost:9200')


async def structure_ncbi_geo():
    search = Search(using=client, index='ncbi_geo')
    for doc in search.params(scroll='1d').scan():
        _id = doc.meta.id
        if not client.exists(index='ncbi_geo_transformed', id=_id):
            dic = doc.to_dict()
            url = 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' + _id
            try:
                jsld = await transform(dic, url, _id)
            except Exception as e:
                client.index(index='ncbi_geo_error', id=_id, body={'exception': str(e)})
            else:
                client.index(index='ncbi_geo_transformed', id=_id, body=jsld)

logging.basicConfig(level='INFO')
loop = asyncio.get_event_loop()
loop.run_until_complete(structure_ncbi_geo())
