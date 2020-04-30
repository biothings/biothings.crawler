from biothings.web.pipeline import ESResultTransform
from biothings.web.pipeline import ESQueryBuilder


from biothings.utils.web.es_dsl import AsyncSearch


class MPQueryBuilder(ESQueryBuilder):

    def default_string_query(self, q, options):

        return AsyncSearch().from_dict({
            "query": {
                "dis_max": {
                    "queries": [
                        {
                            "query_string": {
                                "query": q,
                                "fields": ["name^6", "description^3"]
                            }
                        },
                        {
                            "query_string": {
                                "query": q
                            }
                        }
                    ]
                }
            }
        })


DATASOURCES = (
    'harvard_dataverse',
    'discovery',
    'ncbi_geo',
    'omicsdi',
    'zenodo',
    'immport',
    'nyu')


class MPResultTransformer(ESResultTransform):

    def transform_hit(self, path, doc, options):

        if path == '':
            doc.pop('_type', None)
            doc.pop('sort', None)     # added when using sort
            doc.pop('_node', None)    # added when using explain
            doc.pop('_shard', None)   # added when using explain
            for source in DATASOURCES:
                if source in doc['_index']:
                    doc['_index'] = source
                    break