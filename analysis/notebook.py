
# %%
from collections import defaultdict, Counter
from functools import partial
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch
from biothings_schema import Schema

# %%
schema = Schema()
dataset = schema.get_class("schema:Dataset")

# %%
properties = [
    {
        'name': prop['label'],
        'types': [kls.name for kls in prop['range']]
    }
    for prop in dataset.list_properties(group_by_class=False)
]

# %%
property_names = [
    prop['label'] for prop in dataset.list_properties(
        class_specific=False, group_by_class=False)
]

# %%
client = Elasticsearch('su07:9199')
indicies = ('zenodo', 'harvard_dataverse', 'omicsdi')
result = defaultdict(partial(defaultdict, Counter))

count = 0
for index in indicies:
    search = Search(using=client, index=index)
    for doc in search.scan():
        dic = doc.to_dict()
        for key, value in dic.items():
            if key in property_names:
                result[index][key][type(value).__name__] += 1
            else:
                result[index]['__EE__'][type(value).__name__] += 1
        count += 1
        if count % 10000 == 0:
            print(count)
        # break


# %%
for index in result:
    print('\t'.join((index.ljust(20), 'string', 'list', 'object', 'percentage')))
    for key in dict(sorted(result[index].items())):
        print('\t'.join([
            key.ljust(20),
            str(result[index][key]['str']),
            str(result[index][key]['list']),
            str(result[index][key]['dict']),
            "{:7.2%}".format(
                sum(result[index][key].values()) /
                Search(using=client, index=index).count())
        ]))
    print()


# %%
