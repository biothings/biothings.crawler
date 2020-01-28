"""
    ImmPort Metadata Translation

    The following fields are ignored:

        "Gender Included",
        "Subjects Number",
        "Data Completeness",
        "Brief Description",
        "Objectives",
        "Endpoints"
"""
import logging
import time

from elasticsearch_dsl import Search
from elasticsearch_dsl.connections import connections

from helper import pmid_to_citation, pmid_to_funding, transform

connections.create_connection(hosts=['su07:9199'])


def PI_translation(value):

    creators = []
    for individual in value.split('; '):
        segments = len(individual.split(" - "))
        if segments != 2:
            logging.warning('Cannot transform %s.', individual)
        else:
            creators.append({
                "@type": "Person",
                "name": individual.split(" - ")[0],
                "affiliation": individual.split(" - ")[1]
            })
    return {
        "creator": creators,
    }


IMMPORT_MAPPINGS = {
    "Accession": "identifier",
    "Title": "name",
    "Start Date": "datePublished",
    "Detailed Description": "description",
    "_id": "url",
    "PI": PI_translation,
    "Condition Studied": lambda value: {
        "keywords": value.split(', ')
    },  # Could possibly go into variableMeasured,
    # but is more subjectMeasured than variable...
    "DOI": lambda value: {
        "sameAs": f"https://www.doi.org/{value}"
    },
    "Download Packages": lambda value: {
        "distribution": [{
            "@type": "DataDownload",
            "contentUrl": value}]
    },
    "Contract/Grant": lambda value: {
        "funder": [{
            "@type": "Organization",
            "name": value
        }]
    }
}


def main():
    client = connections.get_connection()
    search = Search(index='immport')

    for doc in search.params(scroll='1d').scan():

        _id = doc.meta.id
        dic = doc.to_dict()

        logging.info('%s', _id)

        try:
            doc = transform(dic, IMMPORT_MAPPINGS)
        except Exception as e:
            logging.warning(e)

        metadata = {
            "includedInDataCatalog": {
                "@type": "DataCatalog",
                "name": "ImmPort",
                "url": "http://immport.org/"
            }
        }
        doc.update(metadata)

        try:
            funding = []
            for pmid in dic.get('Pubmed Id', []):
                funding += pmid_to_funding(pmid)
        except Exception as e:
            logging.warning(e)
        else:
            if funding:
                doc['funding'] = funding

        try:
            citations = []
            for pmid in dic.get('Pubmed Id', []):
                citations.append(pmid_to_citation(pmid))
        except Exception as e:
            logging.warning(e)
        else:
            if citations:
                doc['citation'] = citations

        client.index(index='immport_transformed', id=_id, body=doc)
        time.sleep(0.2)  # throttle request rates


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
