# %% [markdown]
# ## Datasets
# http://su07:9199/_cat/indices?v
# ### Zenodo
# http://su07:9199/zenodo/_search?q=*
# ### Harvard Dataverse
# http://su07:9199/harvard_dataverse/_search?q=*
# ### NCBI Geo
# http://su07:9199/ncbi_geo/_search?q=*
# ### Omicsdi
# http://su07:9199/omicsdi/_search?q=*


# %% [markdown]
# ## Question
# Thread #cvisb andrew  21 days ago
# > For each repository, show how many datasets have each metadata field populated.

# %%
from collections import defaultdict, Counter
from functools import partial
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch
from biothings_schema import Schema

schema = Schema()
dataset = schema.get_class("schema:Dataset")
properties = sorted([
    prop['label'] for prop in dataset.list_properties(
        class_specific=False, group_by_class=False)
])

# %%
client = Elasticsearch('su07:9199')
indicies = ('zenodo', 'omicsdi', 'harvard_dataverse','ncbi_geo_transformed')

result = defaultdict(partial(defaultdict, Counter))

count = 0
for index in indicies:
    search = Search(using=client, index=index)
    for doc in search.scan():
        dic = doc.to_dict()
        for key, value in dic.items():
            if key in properties:
                result[index][key][type(value).__name__] += 1
            else:
                result[index]['__EE__'][type(value).__name__] += 1
        count += 1
        if count % 10000 == 0:
            print(count)

# %%
for index in result:
    for key in result[index]:
        result[index][key]['ratio'] = \
            (sum(result[index][key].values()) /
             Search(using=client, index=index).count())

# %%
for index in result:
    print('\t'.join((index.ljust(20), 'string', 'list', 'object', 'percentage')))
    for key in dict(sorted(result[index].items())):
        print('\t'.join([
            key.ljust(20),
            str(result[index][key]['str']),
            str(result[index][key]['list']),
            str(result[index][key]['dict']),
            "{:7.2%}".format(result[index][key]['ratio'])
        ]))
    print()


# %%
print('\t'.join(('properties'.ljust(25), ) + tuple(index.ljust(10) for index in indicies)))
for property in properties:
    print(property.ljust(25), end='\t')
    for index in indicies:
        if property in result[index]:
            print("{:<10.2%}".format(result[index][property]['ratio']), end='\t')
        else:
            print('-'.ljust(10), end='\t')
    print()


# %%
