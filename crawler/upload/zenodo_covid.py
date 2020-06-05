"""
python -m crawler.upload
       --uploader=zenodo_covid
       --src-index=crawler_zenodo_covid
       --dest-index=outbreak-resources-zenodo"
"""

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
    @property
    def INDEX_MAPPINGS(self):
        return {
            "properties": requests.get(MAPPING_URL).json()
        }

    def transform_doc(self, doc):

        # add the property curatedBy
        doc['curatedBy'] = {
            '@type': 'Organization',
            'name': 'Zenodo',
            'url': 'https://zenodo.org/communities/covid-19/',
            'versionDate': datetime.now()
        }

        # date property
        # TODO

        # Force all @type:ScholarlyArticle to be @type:Publication
        if doc['@type'] == 'ScholarlyArticle':
            doc['@type'] = 'Publication'

        # make creator.affiliation an object
        try:
            for creator in doc['creator']:
                if isinstance(creator['affiliation'], str):
                    creator['affiliation'] = {
                        'name': creator['affiliation']
                    }
        except KeyError:
            pass

        return doc

    def extract_id(self, doc):

        return 'zenodo.' + doc.pop('_id').split('.')[-1]
