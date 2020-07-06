"""
    Trasnform NCBI Geo to Schema.org Structured Metadata

    Assume the original _id needs to be transformed
    from _id = GSE9 -> _id = URL the data is scraped from

    'Citation(s)' field may not exist. If exist, must be str.
    May include a number, or numbers separated by ', '.
"""

### TODO NEED TO RESTRUCTRUE

import logging
import os
import time

from elasticsearch_dsl import Search
from elasticsearch_dsl.connections import connections

from helper import (pmid_to_citation, pmid_to_funding, pmid_with_eutils,
                    transform)

connections.create_connection(hosts=['localhost:9199'])
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

NCBI_MAPPINGS = {
    "Title": "name",
    "Organism": "organism",
    "Experiment type": "measurementTechnique",
    "Summary": "description",
    "Contributor(s)": lambda value: {
        "creator": [{
            "@type": "Person",
            "name": individual
        } for individual in value.split(', ')]
    },
    "Submission date": "datePublished",
    "Last update date": "dateModified",
    "Organization": lambda value: {
        "publisher": {
            "@type": "Organization",
            "name": value
        }
    }
}


def main():
    client = connections.get_connection()
    search = Search(index='ncbi_geo')

    for doc in search.params(scroll='1d').scan(): # pylint: disable=no-member

        _id = doc.meta.id
        dic = doc.to_dict()

        url = 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' + _id

        # if not client.exists(index='ncbi_geo_transformed', id=_id):

        logging.info('%s', _id)

        ### Transform data fields ###

        try:
            doc = transform(dic, NCBI_MAPPINGS)
        except Exception as e:
            logging.warning(e)

        ### Add metadata fixtures ###

        meta = {
            "identifier": _id,
            "distribution": {
                "@type": "dataDownload",
                "contentUrl": url
            },
            "includedInDataCatalog": {
                "@type": "DataCatalog",
                "name": "NCBI GEO from Metadataplus",
                "url": "https://www.ncbi.nlm.nih.gov/geo/"
            }
        }
        doc.update(meta)

        if 'Citation(s)' in dic:

            if 'API_KEY' in os.environ:

                funding = []
                citations = []

                for pmid in dic['Citation(s)'].split(','):
                    pmid = pmid.strip()
                    grants, citation = pmid_with_eutils(pmid)
                    funding += grants
                    citations.append(citation)

                if funding:
                    doc['funding'] = funding

                if citations:
                    doc['citation'] = citations

                time.sleep(0.1)  # throttle request rates

            else:

                ### Add funding field ###

                try:
                    funding = []
                    for pmid in dic['Citation(s)'].split(','):
                        pmid = pmid.strip()
                        funding += pmid_to_funding(pmid)
                except Exception as e:
                    logging.warning(e)
                else:
                    if funding:
                        doc['funding'] = funding

                ### Add citation field ###

                try:
                    citations = []
                    for pmid in dic['Citation(s)'].split(','):
                        pmid = pmid.strip()
                        citations.append(pmid_to_citation(pmid))
                except Exception as e:
                    logging.warning(e)
                else:
                    if citations:
                        doc['citation'] = citations

                time.sleep(0.2)  # throttle request rates


        client.index(index='transformed_ncbi_geo', id=url, body=doc)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
