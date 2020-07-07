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

from . import CrawlerESUploader
from .helper import pmid_to_citation, pmid_to_funding


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
        }).delete_unused_keys()

        try:
            funding = []
            for pmid in doc.get('Pubmed Id', []):
                funding += pmid_to_funding(pmid)
        except Exception as e:
            logging.warning(e)
        else:
            if funding:
                doc['funding'] = funding

        try:
            citations = []
            for pmid in doc.get('Pubmed Id', []):
                citations.append(pmid_to_citation(pmid))
        except Exception as e:
            logging.warning(e)
        else:
            if citations:
                doc['citation'] = citations

        return dict(sorted(doc.items()))
