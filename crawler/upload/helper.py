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
    body = requests.get(base_api_url, params=parameters, timeout=15).text
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
    # ANSI/NISO Z39.29-2005 (R2010), I don't have time to implement the full specs, and I cannot find any library
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
    # func. to build string
    def construct_cite_partial(xml, features):
        partial_cite = ''
        for feature, template in features:
            if xml.find(feature) is not None:
                text = xml.find(feature).text
                partial_cite += template.format(text)
        return partial_cite

    citation += construct_cite_partial(root, (
        ('.//MedlineCitation/Article/ArticleTitle', '{} '),
        ('.//MedlineCitation/MedlineJournalInfo/MedlineTA', '{}'),
    ))
    journal_date_base = './/MedlineCitation/Article/Journal/JournalIssue/PubDate/'
    for p in ['Year', 'Month', 'Day']:
        if root.find(journal_date_base + p) is not None:
            text = root.find(journal_date_base + p).text  # TODO: after 3.8, use the Walrus operator so it's cleaner
            # The space is added BEFORE the date, so the previous space was removed
            citation += f' {text}'
        else:
            # If we can't find more date fields, end
            break
    citation += ';'  # add the semicolon

    citation += construct_cite_partial(root, (
        ('.//MedlineCitation/Article/Journal/JournalIssue/Volume', '{}'),
        ('.//MedlineCitation/Article/Journal/JournalIssue/Issue', '({})'),
        ('.//MedlineCitation/Article/Pagination/MedlinePgn', ':{}.'),
        ('.//MedlineCitation/PMID', ' PMID: {}')
    ))

    return grants, citation
