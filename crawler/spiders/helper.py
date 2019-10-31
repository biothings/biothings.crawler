import logging

from extruct.jsonld import JsonLdExtractor


class JsonLdMixin:

    def extract_jsonld(self, response, _id=None):

        jslds = JsonLdExtractor().extract(response.body)

        for jsld in jslds:
            if _id:
                jsld['_id'] = _id
            logging.debug(jsld)
            yield jsld
