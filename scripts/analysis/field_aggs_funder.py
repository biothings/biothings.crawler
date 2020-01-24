# %% [markdown]
# ## Question
# Thread #cvisb andrew Sep 27th at 4:07 PM
# > generate a list of values under funder with counts?

# %%
from collections import defaultdict, Counter
from functools import partial
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch
from pprint import pprint
# %%
client = Elasticsearch('su07:9199')
# indicies = ('zenodo', 'omicsdi', 'harvard_dataverse')
index = 'ncbi_geo'

result = defaultdict(partial(defaultdict, Counter))


def lookup(fieldname, missing=False):
    count = 0
    search = Search(using=client, index=index)
    for doc in search.scan():
        dic = doc.to_dict()
        if missing:
            condition = fieldname not in dic
        else:
            condition = fieldname in dic
        if condition:
            print(doc.meta.id)
            pprint(dic[fieldname])
            print()
            count += 1
        if count == 10:
            break


# %%
# citations missings field
lookup('Citation missing')


# %%
# Citation field prerendered
lookup('Citation')

# %%
# Organism/organisms
lookup('Organism')
print('------------')
lookup('Organisms')
print('------------')
count = 0
search = Search(using=client, index=index)
for doc in search.scan():
    dic = doc.to_dict()
    if 'Organism' not in dic and 'Organisms' not in dic:
        print(doc.meta.id)
        pprint(dic)
        print()
        count += 1
    if count == 3:
        break


# %%
lookup('Organization')
lookup('Organization name')

# %%
