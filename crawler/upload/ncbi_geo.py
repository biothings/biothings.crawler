"""
    Trasnform NCBI Geo to Schema.org Structured Metadata

    Assume the original _id needs to be transformed
    from _id = GSE9 -> _id = URL the data is scraped from

    'Citation(s)' field may not exist. If exist, must be str.
    May include a number, or numbers separated by ', '.
"""

import os
import time

from . import CrawlerESUploader
from .helper import get_funding_cite_from_eutils


class NCBIGeoUploader(CrawlerESUploader):
    NAME = 'ncbi_geo'
    BIOTHING_TYPE = 'dataset'  # TODO: is this the proper type?
    BIOTHING_VERSION = 'c1.0'

    # keep default settings
    # INDEX_SETTINGS = {}
    # INDEX_MAPPINGS = {}

    def extract_id(self, doc):
        # the _id is left in the document
        return 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' + doc['_id']

    def transform_doc(self, doc):
        # Update document using transforms
        doc = doc.transform_keys_values({
            "Contributor(s)": lambda value: {
                "creator": [{
                    "@type": "Person",
                    "name": individual
                } for individual in value.split(', ')]
            },
            "Organization": lambda value: {
                "publisher": {
                    "@type": "Organization",
                    "name": value
                }
            }
        }, ignore_key_error=True).rename_keys({
            "Title": "name",
            "Organism": "organism",
            "Experiment type": "measurementTechnique",
            "Summary": "description",
            "Submission date": "datePublished",
            "Last update date": "dateModified",
        }, ignore_key_error=True)

        # Perform additional changes
        _id = doc.pop('_id')  # obtain and remove this from the original document
        url = 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' + _id
        meta = {
            "@context": "http://schema.org/",
            "@type": "Dataset",
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

        if 'Citation(s)' in doc:
            # TODO: consider using pmid_to_citation() for citations or fix the formatting from
            #  get_funding_cite_from_eutils to match
            funding = []
            citations = []
            if 'API_KEY' in os.environ:
                api_key = os.environ['API_KEY']
            else:
                api_key = None
            for pmid in doc['Citation(s)'].split(','):
                pmid = pmid.strip()
                grants, citation = get_funding_cite_from_eutils(pmid, api_key)
                # throttle request rates, NCBI says up to 10 requests per second with API Key, 3/s without.
                if api_key is not None:
                    time.sleep(0.1)
                else:
                    time.sleep(0.35)
                funding += grants
                citations.append(citation)
            if funding:
                doc['funding'] = funding
            if citations:
                doc['citation'] = citations

        doc.delete_unused_keys()
        return doc

# async def structure_ncbi_geo():
#     global count
#     search = Search(using=client, index=TARGET_INDEX)
#     search = search.params(scroll='1d', sort=['_id'])
#     for doc in search.scan():
#         if 'funding' in doc:
#             new_list = []
#             for funding in doc.funding:
#                 if funding['funder']['name'] and len(funding['identifier']) > 3:
#                     new_list.append(funding)
#             logging.info("%s -> %s", len(doc.funding), len(new_list))
#             if not new_list:
#                 new_list = None
#             client.update(
#                 index=TARGET_INDEX,
#                 id=doc.meta.id,
#                 body={
#                     "doc": {
#                         "funding": new_list}})
# loop = asyncio.get_event_loop()
# loop.run_until_complete(structure_ncbi_geo())
