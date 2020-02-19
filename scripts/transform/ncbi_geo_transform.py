'''
    Trasnform NCBI Geo to Schema.org Structured Metadata

    Assume the original _id needs to be transformed
    from _id = GSE9 -> _id = URL the data is scraped from

    'Citation(s)' field may not exist. If exist, must be str.
    May include a number, or numbers separated by ', '.
'''

import logging
import time

from elasticsearch_dsl import Search
from elasticsearch_dsl.connections import connections

from helper import pmid_to_citation, pmid_to_funding, transform

connections.create_connection(hosts=['localhost:9199'])

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

    for doc in search.params(scroll='1d').scan():

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

        client.index(index='transformed_ncbi_geo', id=url, body=doc)
        time.sleep(0.2)  # throttle request rates


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
