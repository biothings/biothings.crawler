import copy
import requests

from .immport import ImmPortUploader
from .zenodo_covid import ZenodoCovidUploader, MAPPING_URL
from .tdoc import TransformDoc

_mapping = {
            "properties": requests.get(MAPPING_URL).json(),
            "dynamic": False
        }


class ImmPortCovidUploader(ImmPortUploader):
    NAME = 'immport_covid'
    # override default settings
    # duplicate the Zenodo ones, as they are the same
    INDEX_SETTINGS = copy.deepcopy(ZenodoCovidUploader.INDEX_SETTINGS)
    INDEX_MAPPINGS = copy.deepcopy(_mapping)

    def transform_doc(self, doc):
        doc = super(ImmPortCovidUploader, self).transform_doc(doc)
        doc = TransformDoc(doc)
        # make creator.affiliation an object
        # FIXME: copy paste from zenodo_covid for now
        if 'creator' in doc and isinstance(doc['creator'], list):
            for creator in doc['creator']:
                if 'affiliation' in creator and isinstance(
                        creator['affiliation'], str):
                    creator['affiliation'] = {
                        'name': creator['affiliation']
                    }
        # remove citation field
        doc.delete_keys('citation')
        return doc
