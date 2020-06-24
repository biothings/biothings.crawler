"""
python -m crawler.upload --uploader=zenodo_covid --src-index=crawler_zenodo_covid --dest-index=outbreak-resources-zenodo
"""
import re
from datetime import datetime

import requests

from . import CrawlerESUploader

MAPPING_URL = 'https://raw.githubusercontent.com/SuLab/outbreak.info-resources/master/outbreak_resources_es_mapping.json'


class ZenodoCovidUploader(CrawlerESUploader):

    NAME = 'zenodo_covid'
    INDEX_SETTINGS = {
        "query": {
            "default_field": "all"
        },
        "default_pipeline": "resources-common",
        "analysis": {
            "normalizer": {
                "keyword_lowercase_normalizer": {
                    "filter": [
                        "lowercase"
                    ],
                    "type": "custom",
                    "char_filter": []
                }
            },
            "analyzer": {
                "string_lowercase": {
                    "filter": "lowercase",
                    "tokenizer": "keyword"
                },
                "whitespace_lowercase": {
                    "filter": "lowercase",
                    "tokenizer": "whitespace"
                }
            }
        }
    }
    BIOTHING_TYPE = 'resources'
    BIOTHING_VERSION = 'c1.0'

    @property
    def INDEX_MAPPINGS(self):
        return {
            "properties": requests.get(MAPPING_URL).json(),
            "dynamic": False
        }

    def transform_doc(self, doc):

        # add the property curatedBy
        doc['curatedBy'] = {
            '@type': 'Organization',
            'name': 'Zenodo',
            'url': 'https://zenodo.org/communities/covid-19/',
            'versionDate': datetime.now()
        }

        # Force all @type:ScholarlyArticle to be @type:Publication
        if doc.get('@type') == 'ScholarlyArticle':
            doc['@type'] = 'Publication'

        # make creator.affiliation an object
        if 'creator' in doc and isinstance(doc['creator'], list):
            for creator in doc['creator']:
                if 'affiliation' in creator and isinstance(creator['affiliation'], str):
                    creator['affiliation'] = {
                        'name': creator['affiliation']
                    }

        # fix fake list
        if 'keywords' in doc and len(doc['keywords']) == 1:
            doc['keywords'] = re.split(r', |,|; |;', doc['keywords'][0])

        # fix license field
        if 'license' in doc and isinstance(doc['license'], dict):
            if isinstance(doc['license'].get('license'), str):
                doc['license'] = doc['license']['license']
            else: # unsupported license object
                doc.pop('license')

        return doc

    def extract_id(self, doc):

        return 'zenodo.' + doc.pop('_id').split('.')[-1]
