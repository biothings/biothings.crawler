
### TODO NEED TO RESTRUCTRUE

from datetime import datetime

class ZenodoCovidUploader:
    
    def transform(self, doc):

        # add the property curatedBy
        doc['curatedBy'] = {
            '@type': 'Organization',
            'name': 'Zenodo',
            'url': 'https://zenodo.org/communities/covid-19/',
            'versionDate': datetime.now()
        }
        # change _id or identifier to be something prettier
        doc['_id'] = 'zenodo.' + doc.pop('_id').split('.')[-1]
        # date property
        # TODO
        # Force all @type:ScholarlyArticle to be @type:Publication
        if doc['@type'] == 'ScholarlyArticle':
            doc['@type'] = 'Publication'
