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

import os
import time
import logging
from datetime import datetime

from . import CrawlerESUploader
from .helper import batch_get_pmid_eutils


# TODO: properly setup logger


class ImmPortUploader(CrawlerESUploader):
    NAME = 'immport'
    BIOTHING_TYPE = 'dataset'
    BIOTHING_VERSION = 'c1.0'

    # keep default settings
    # INDEX_SETTINGS = {}
    # INDEX_MAPPINGS = {}

    # _id is not transformed

    @staticmethod
    def pi_translation(value):

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

    def transform_doc(self, doc):
        # In this implementation, most transforms are done using
        # the helper functions from the `TransformDoc` class.
        #
        # As shown afterwards, this can also mix and match with updating
        # the document directly.

        doc['curatedBy'] = {
            '@type': 'Organization',
            'curationDate': datetime.now().strftime('%Y-%m-%d'),
            'name': 'ImmPort',
            'url': f"https://www.immport.org/shared/study/{doc['Accession']}",
        }

        doc.transform_keys_values({
            "PI": self.pi_translation,
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
        }, ignore_key_error=True).rename_keys({
            "Accession": "identifier",
            "Title": "name",
            "Start Date": "datePublished",
            "Detailed Description": "description",
            "_id": "url",
        }, ignore_key_error=True).update({
            "@context": "http://schema.org/",
            "@type": "Dataset",
            "includedInDataCatalog": {
                "@type": "DataCatalog",
                "name": "ImmPort",
                "url": "http://immport.org/"
            }
        })

        funding = []
        citations = []
        if 'API_KEY' in os.environ:
            api_key = os.environ['API_KEY']
        else:
            api_key = None
        pmids = [pmid.strip() for pmid in doc.get('Pubmed Id', [])]
        eutils_info = batch_get_pmid_eutils(pmids, timeout=15.0, api_key=api_key)
        # throttle request rates, NCBI says up to 10 requests per second with API Key, 3/s without.
        if api_key is not None:
            time.sleep(0.1)
        else:
            time.sleep(0.35)
        for pmid in pmids:
            if pmid not in eutils_info:
                continue  # somehow there could be malformed IDs
                # see SDY1655 on Oct 18, 2020 ~ 3:00PM PDT
                # available on the wayback machine
            grants = eutils_info[pmid]['grants']
            citation = eutils_info[pmid]['citation']
            funding += grants
            citations.append(citation)
        if funding:
            doc['funding'] = funding
        if citations:
            doc['citation'] = citations

        doc.delete_unused_keys()
        return dict(sorted(doc.items()))
