# %% [markdown]
# ## Question
# Thread #cvisb andrew Sep 27th at 4:07 PM
# > generate a list of values under funder with counts?

# %%
from collections import defaultdict, Counter
from functools import partial
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch

# %%
client = Elasticsearch('su07:9199')
# indicies = ('zenodo', 'omicsdi', 'harvard_dataverse')
index = 'harvard_dataverse'

result = defaultdict(partial(defaultdict, Counter))


def every_funder():
    search = Search(using=client, index=index)
    for doc in search.scan():
        dic = doc.to_dict()
        if 'funder' in dic:
            for funder in dic['funder']:
                yield funder


# 'funder' field is a list of objects
for funder in every_funder():
    assert isinstance(funder, dict)

# %%
counter = Counter()
# irregular data
for funder in every_funder():
    if len(funder) != 2 \
        or not funder['name'] \
            or funder['@type'] != 'Organization':
        counter[str(funder)] += 1
counter.most_common()

# %%
counter = Counter()
for funder in every_funder():
    name = funder.get('name')
    if name:
        counter[name] += 1
counter.most_common(50)


# %%
funder_of_interest = 'United States Department of Health and Human Services. National Institutes of Health. National Institute on Aging'
search = Search(using=client, index=index)
ids = []
for doc in search.scan():
    dic = doc.to_dict()
    if 'funder' in dic:
        for funder in dic['funder']:
            if funder.get('name') == funder_of_interest:
                ids.append(doc.meta.id)
print(ids)
print(len(ids))

# %%
