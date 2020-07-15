from typing import Optional, Tuple, List, Iterable, Dict
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


def batch_get_pmid_eutils(pmids: Iterable[str], timeout: float, api_key: Optional[str] = None) -> Dict[str, Dict]:
    """Use pmid to retrieve both citation and funding info in batch

    :param pmids: A list of PubMed PMIDs
    :param timeout: Time before the connection times out
    :param api_key: API Key from NCBI to access E-utilities
    :return: A dictionary containing pmids and the corresponding dictionary with citation and grant info.
    """
    base_api_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
    parameters = {
        'db': 'pubmed',
        'id': ','.join(pmids),
        'retmode': 'xml'
    }
    if api_key is not None:
        parameters.update({'api_key': api_key})
    body = requests.get(base_api_url, params=parameters, timeout=timeout).text
    root = ElementTree.fromstring(body)
    ret = {}

    # func. to build partial string
    def construct_cite_partial(xml, features):
        partial_cite = ''
        for feature, template in features:
            if xml.find(feature) is not None:
                text = xml.find(feature).text
                partial_cite += template.format(text)
        return partial_cite

    for pubmed_article in root.findall('.//PubmedArticle'):
        pmid = pubmed_article.find('.//MedlineCitation/PMID').text
        grants = []
        for grant_element in pubmed_article.findall('.//Grant'):
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
        for author in pubmed_article.findall('.//Author'):
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

        citation += construct_cite_partial(pubmed_article, (
            ('.//MedlineCitation/Article/ArticleTitle', '{} '),
            ('.//MedlineCitation/MedlineJournalInfo/MedlineTA', '{}'),
        ))
        journal_date_base = './/MedlineCitation/Article/Journal/JournalIssue/PubDate/'
        for p in ['Year', 'Month', 'Day']:
            if pubmed_article.find(journal_date_base + p) is not None:
                text = pubmed_article.find(
                    journal_date_base + p).text  # TODO: after 3.8, use the Walrus operator so it's cleaner
                # The space is added BEFORE the date, so the previous space was removed
                citation += f' {text}'
            else:
                # If we can't find more date fields, end
                break
        citation += ';'  # add the semicolon

        citation += construct_cite_partial(pubmed_article, (
            ('.//MedlineCitation/Article/Journal/JournalIssue/Volume', '{}'),
            ('.//MedlineCitation/Article/Journal/JournalIssue/Issue', '({})'),
            ('.//MedlineCitation/Article/Pagination/MedlinePgn', ':{}.'),
            ('.//MedlineCitation/PMID', ' PMID: {}')
        ))
        ret[pmid] = {
            'grants': grants,
            'citation': citation
        }
    return ret


def get_funding_cite_from_eutils(pmid: str, api_key: Optional[str] = None) -> Tuple[List[str], str]:
    """Use pmid to retrieve both citation and funding info

    :param pmid: PubMed PMID
    :param api_key: API Key from NCBI to access E-utilities
    :return: A list of GrantIDs and a string for Citation
    """
    doc = batch_get_pmid_eutils([pmid], timeout=15.0, api_key=api_key)
    grants = doc[pmid]['grants']
    citation = doc[pmid]['citation']

    return grants, citation
