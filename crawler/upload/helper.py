from typing import Optional, Tuple, List
import xml.etree.ElementTree as ElementTree

import requests
from scrapy.selector import Selector


def transform(doc, mappings):

    _doc = {
        "@context": "http://schema.org/",
        "@type": "Dataset"
    }

    for key, value in doc.items():
        if key in mappings:
            if isinstance(mappings[key], str):
                _doc[mappings[key]] = value
            elif callable(mappings[key]):
                _doc.update(mappings[key](value))
            else:
                raise RuntimeError()

    return dict(sorted(_doc.items()))


def pmid_to_citation(pmid):
    '''
    Use pmid to find citation string
    '''
    url = 'https://www.ncbi.nlm.nih.gov/sites/PubmedCitation?id=' + pmid
    body = requests.get(url, timeout=5).text
    citation = Selector(text=body).xpath('string(/)').get()
    return citation.replace(u'\xa0', u' ')


def get_funding_cite_from_eutils(pmid: str, api_key: Optional[str] = None) -> Tuple[List[str], str]:
    """Use pmid to retrieve both citation and funding info

    :param pmid: PubMed PMID
    :param api_key: API Key from NCBI to access E-utilities
    :return: A list of GrantIDs and a string for Citation
    """
    # TODO: The API endpoint supports batch querying, and we aren't using it.
    base_api_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
    parameters = {
        'db': 'pubmed',
        'id': str(pmid),
        'retmode': 'xml'
    }
    if api_key is not None:
        parameters.update({'api_key': api_key})
    body = requests.get(base_api_url, params=parameters).text
    root = ElementTree.fromstring(body)

    assert root

    # funding field

    grants = []
    for grant_element in root.findall('.//Grant'):

        grant = {}
        if grant_element.find('Agency') is not None:
            grant['funder'] = {
                '@type': 'Organization',
                'name': grant_element.find('Agency').text
            }

        if grant_element.find('GrantID') is not None:
            grant['identifier'] = grant_element.find('GrantID').text

        if grant:
            grants.append(grant)

    # citation field
    citation = ''

    # author string
    authors = []
    for author in root.findall('.//Author'):
        lastname = author.find('LastName').text
        initials = author.find('Initials').text
        authors.append(f"{lastname} {initials}")

    if len(authors) > 4:
        string = ', '.join(authors[:4])
        string += ' et al. '
        citation += string

    elif len(authors) > 1:
        string = ', '.join(authors)
        string += '. '
        citation += string

    elif len(authors) == 1:
        citation += authors[0]
        citation += '. '

    # the remaining string
    features = (
        ('.//MedlineCitation/Article/ArticleTitle', '{} '),
        ('.//MedlineCitation/MedlineJournalInfo/MedlineTA', '{} '),
        ('.//MedlineCitation/Article/Journal/JournalIssue/PubDate/Year', '{} '),
        ('.//MedlineCitation/Article/Journal/JournalIssue/PubDate/Month', '{};'),
        ('.//MedlineCitation/Article/Journal/JournalIssue/Volume', '{}'),
        ('.//MedlineCitation/Article/Journal/JournalIssue/Issue', '({})'),
        ('.//MedlineCitation/Article/Pagination/MedlinePgn', ':{}'),
    )

    for feature, template in features:
        if root.find(feature) is not None:
            text = root.find(feature).text
            citation += template.format(text)

    return grants, citation
