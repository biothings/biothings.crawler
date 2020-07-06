"""
Why Reindex/Upload Again to ES

When items are first crawled, they are stored in ES with mapping disabled.
So that field type conflicts would not interrupt the crawling process.

To make the crawled items searchable and aggregatable, reindexing is necessary.

"""

from datetime import datetime
import logging
import os
from typing import Union

from elasticsearch import Elasticsearch
from elasticsearch.helpers import reindex, scan

from .tdoc import TransformDoc


uploaders = {
    # 'default': <class crawler.upload.CrawlerESUploader>,
    # 'dataset': <class crawler.upload.CrawlerDatasetESUploader>
}


class CrawlerESUploader:

    NAME = 'default'

    #---------------------------#
    #           Index           #
    #---------------------------#

    INDEX_MAPPINGS = {
        # "<field_name>": "<elasticsearch_definition>"
    }
    INDEX_SETTINGS = {
        # "query.default_field": "all",
        # "analysis": { ... }
    }
    # create a new index if mapping conflicts happen
    INDEX_FIXCONFLICTS = False
    INDEX_FIX_MAX_NUMS = 10

    #---------------------------#
    #          Metadata         #
    #---------------------------#

    BIOTHING_TYPE = 'biothing'
    BIOTHING_VERSION = 'c1.0'

    def __init__(self, **kwargs):
        self.metadata = DataMetadata(self)
        self.indexing = DataReindex(self, **kwargs)

    def __init_subclass__(cls):
        super().__init_subclass__()
        uploaders[cls.NAME] = cls

    def upload(self):
        """
        Upload the document and apply transformations.
        This is primarily how to interact with this class.
        """
        # reindex as-is
        if not self.INDEX_MAPPINGS and self.extract_id is CrawlerESUploader.extract_id:
            self.indexing.reindex()
            return

        # apply transformation
        for doc in self.indexing.scan():
            _id = self.extract_id(doc)
            _source = self.transform_doc(TransformDoc(doc))
            self.indexing.index(_id, _source)

    def extract_id(self, doc):
        """
        Determine the _id of the new document.
        By default use the original _id from source index.
        The document input will look like this:
        {
            '_id': '114553807', # original id
            'taxid': 8167,
            'symbol': 'cdk2',
            'name': 'cyclin dependent kinase 2
        }
        Suppose we would like 'taxid' to be the new id:
        |
        |   return doc['taxid'] # assume field exists
        |
        Return None to generate a random id.
        """
        return doc.pop('_id', None)

    def transform_doc(self, doc: TransformDoc) -> dict:
        """
        Modify documents after the quick transform phase.
        """
        # override here
        raise NotImplementedError


class CrawlerDatasetESUploader(CrawlerESUploader):

    NAME = 'dataset'

    def transform_doc(self, doc):

        # add common information
        base = {
            "@context": "http://schema.org/",
            "@type": "Dataset"
        }
        base.update(doc)
        return base


class DataReindex:
    """
    An Elasticsearch Reindexing Unit.

    .reindex() performs a simple as-is reindex.
    .scan() and .index() allow customizations in between.

    """

    def __init__(
        self,
        uploader,   # crawler.upload.CrawlerESUploader instance
        src_index, dest_index,  # indices to read from and write to
        src_host=None, dest_host=None  # default host localhost:9200
    ):
        self.src_client = Elasticsearch(src_host)
        self.src_index = src_index

        self.dest_client = Elasticsearch(dest_host)
        self.dest_index = dest_index

        self.uploader = uploader

        mappings = dict(self.uploader.INDEX_MAPPINGS)
        mappings['_meta'] = self.uploader.metadata.get_metadata()

        self.indexing = CrawlerIndices(
            self.dest_client, 
            mappings, # including _meta field
            self.uploader.INDEX_SETTINGS)

        self._valid_indices = set()

    def reindex(self, query=None):
        """
        Reindex all documents from source index that satisfy a given query to the other.
        May be used when no transform defined. And would optionally want to filter results.
        """
        return reindex(
            self.src_client, self.src_index,
            self.dest_index, query, target_client=self.dest_client
        )

    def scan(self):
        """
        An iterator that yields all hits as returned by underlining scroll requests.
        A transformation will be applied, for example, if the original hit is:
        {
            '_index': 'bts_test',
            '_type': 'gene',
            '_id': '114553807',
            '_score': None,
            '_source': {
                'taxid': 8167,
                'symbol': 'cdk2',
                'name': 'cyclin dependent kinase 2
            }
        }
        The document provided by this iterator is:
        {
            '_id': '114553807',
            'taxid': 8167,
            'symbol': 'cdk2',
            'name': 'cyclin dependent kinase 2
        }
        """
        for doc in scan(self.src_client, index=self.src_index):
            _doc = {'_id': doc['_id']}
            _doc.update(doc['_source'])
            yield _doc

    def index(self, _id, _source):
        """
        Creates or updates a document in the destination index.
        """
        if not self.uploader.INDEX_FIXCONFLICTS:
            self.indexing.index(self.dest_index, _id, _source)
            return

        for suffix_num in range(self.uploader.INDEX_FIX_MAX_NUMS):
            try:  # to create file in the first possible index
                self.indexing.index(
                    '_'.join((self.dest_index, suffix_num)),
                    _id, _source, self.dest_index
                )
            except Exception:
                # TODO need to extract the exception,
                # only allow mapping field conflicts.
                # other cases should raise error.
                pass
            else:  # success
                break


class DataMetadata:

    def __init__(self, uploader):
        self.uploader = uploader

    def get_metadata(self):
        return {
            "biothing_type": self.uploader.BIOTHING_TYPE,
            "build_date": datetime.now().isoformat(),
            "build_version": self.uploader.BIOTHING_VERSION,
            "src": {
                self.uploader.NAME: {
                    "code": {
                        "module": self.uploader.__module__,
                        "repo": "https://github.com/biothings/biothings.crawler"
                    }
                }
            },
        }


class CrawlerIndices:

    def __init__(self, client, mappings=None, settings=None):

        self.client = client
        self.settings = settings or {}
        self.mappings = mappings if mappings is not None else dict(enabled="false")
        self._es_version = int(self.client.info()['version']['number'].split('.')[0])
        self._valid_indices = set()

    def index(self, _index, _id, _source, alias=None):

        if _index not in self._valid_indices:
            if self.client.indices.exists(index=_index):
                self.client.indices.delete(index=_index)
            settings = {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
            settings.update(self.settings)
            request_body = {
                "settings": {"index": settings},
                "mappings": self.mappings,
                "aliases": {alias: {}} if alias else {}
            }
            if self._es_version < 7:
                request_body['mappings'] = {
                    "_doc": request_body['mappings']
                }
            self.client.indices.create(
                index=_index,
                body=request_body)
            self._valid_indices.add(_index)

        return self.client.index(index=_index, id=_id, body=_source)


from .zenodo_covid import ZenodoCovidUploader
from .immport import ImmPortUploader
