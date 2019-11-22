''' Add structured schema.org Dataset metadata to NCBI GEO data series page. '''

import json
import logging

import tornado.httpclient
import tornado.ioloop
import tornado.options
import tornado.routing
import tornado.web
from bs4 import BeautifulSoup
from scrapy.selector import Selector
from tornado.options import options

import elasticsearch
from crawler.spiders import NCBIGeoSpider

client = elasticsearch.Elasticsearch(port=9199)

async def transform(doc, url, identifier):

    mappings = {
        "Title": "name",
        "Organism": "organism",
        "Experiment type": "measurementTechnique",
        "Summary": "description",
        "Contributor(s)": lambda value: {
            "creator": [{
                "@type": "Person",
                "name": individual
            } for individual in value.split(', ')]
        },
        "Submission date": "datePublished",
        "Last update date": "dateModified",
        "Organization": lambda value: {
            "publisher": {
                "@type": "Organization",
                "name": value
            }
        },
    }
    _doc = {
        "@context": "http://schema.org/",
        "@type": "Dataset",
        "identifier": identifier,
        "distribution": {
            "@type": "dataDownload",
            "contentUrl": url
        },
        "includedInDataCatalog": {
            "@type": "DataCatalog",
            "name": "NCBI GEO",
            "url": "https://www.ncbi.nlm.nih.gov/geo/"
        }
    }
    pmids = doc.get("Citation(s)")
    if pmids:
        _doc['citation'] = []
        for pmid in pmids.split(', '):

            # funders
            http_client = tornado.httpclient.AsyncHTTPClient()
            url = "https://www.ncbi.nlm.nih.gov/pubmed/" + pmid
            response = await http_client.fetch(url)
            title_xpath = '//*[@id="maincontent"]/div/div[5]/div/div[6]/div[1]/div/h4[4]/text()'
            # grant support section exists
            if Selector(text=response.body.decode()).xpath(title_xpath).get() == 'Grant support':
                xpath = '//*[@id="maincontent"]/div/div[5]/div/div[6]/div[1]/div/ul[4]/li/a/text()'
                supporters = Selector(text=response.body.decode()).xpath(xpath).getall()
                if supporters:
                    identifiers, funders = [], []
                    for supporter in supporters:
                        terms = supporter.split('/')[:-1]
                        identifiers.append(terms[0])
                        funders.append('/'.join(terms[1:]))
                    _doc['funding'] = [
                        {
                            'funder': {
                                '@type': 'Organization',
                                'name': funder
                            },
                            'identifier': identifier.strip(),
                        } for funder, identifier in zip(funders, identifiers)
                    ]

            # citation
            http_client = tornado.httpclient.AsyncHTTPClient()
            citation_url = 'https://www.ncbi.nlm.nih.gov/sites/PubmedCitation?id=' + pmid
            citation_response = await http_client.fetch(citation_url)
            citation_text = Selector(text=citation_response.body.decode()).xpath('string(/)').get()
            _doc['citation'].append(citation_text.replace(u'\xa0', u' '))

    for key, value in doc.items():
        if key in mappings:
            if isinstance(mappings[key], str):
                _doc[mappings[key]] = value
            elif callable(mappings[key]):
                _doc.update(mappings[key](value))
            else:
                raise RuntimeError()

    return dict(sorted(_doc.items()))


class NCBIHandler(tornado.web.RequestHandler):

    @property
    def host(self):
        ''' Publicized content serving host address and port number. '''
        return "{}:{}".format(self.request.host.split(':')[0], options.redirect)


class NCBIProxyHandler(NCBIHandler):

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", self.host)
        self.set_header("Access-Control-Allow-Headers", "*")
        self.set_header('Access-Control-Allow-Methods', 'GET, OPTIONS')

    def options(self):
        self.set_status(204)
        self.finish()

    async def get(self):

        if self.host != self.request.host:
            self.set_status(404)
            self.write("Error Page.")
            return

        root = 'https://www.ncbi.nlm.nih.gov'
        url = root + self.request.uri
        http_client = tornado.httpclient.AsyncHTTPClient()
        response = await http_client.fetch(url, raise_error=False)

        self.set_status(response.code)
        self.set_header('Content-Type', response.headers.get('Content-Type'))
        self.finish(response.body)

class NCBIRandomDatasetExplorer(NCBIHandler):

    async def get(self):
        http_client = tornado.httpclient.AsyncHTTPClient()
        random_id_req = 'http://localhost:{}/api/query?q=__any__&fields=_id&size=1'.format(options.port)
        response = await http_client.fetch(random_id_req)
        _id = json.loads(response.body)['hits'][0]['_id']

        if self.get_argument('redirect', False) is not False:
            self.redirect('//{}/{}'.format(self.host, _id))
        else:
            await NCBIGeoDatasetHandler.get(self, _id)



class NCBIGeoDatasetHandler(NCBIHandler):

    async def get(self, gse_id):

        root = 'https://www.ncbi.nlm.nih.gov'
        path = '/geo/query/acc.cgi?acc='
        url = root + path + gse_id
        http_client = tornado.httpclient.AsyncHTTPClient()
        response = await http_client.fetch(url)
        text = response.body.decode()
        soup = BeautifulSoup(text, 'html.parser')

        # add resource path redirection
        soup.head.insert(0, soup.new_tag(
            'base', href='//{}/geo/query/'.format(self.host)))

        # try to retrieve pre-loaded structured metadata
        try:
            doc = client.get(id=gse_id, index='ncbi_geo_indexed')
        except elasticsearch.ElasticsearchException:
            doc = None
        else:
            doc = doc['_source']
    
        # try to parse raw metadata and do live transform
        if not doc:
            logging.warning('[%s] Cannot retrieve from es.', gse_id)
            try:
                # capture raw metadata
                doc = NCBIGeoSpider().parse(Selector(text=text))
                # transform to structured metadata
                doc = await transform(doc, url, gse_id)
            except Exception:
                logging.warning('[%s] Cannot parse raw metadata.', gse_id)


        if doc:
            # set header message
            message = """
            This page adds structured schema.org <a href="http://schema.org/Dataset">Dataset</a> metadata 
            to the original GEO data series page <a href="{}">{}</a> <a href="{}">Try a different dataset.</a>
            """.format(url, gse_id, '//{}/_random.html?redirect'.format(self.host))
            # add structured metadata
            new_tag = soup.new_tag('script', type="application/ld+json")
            new_tag.string = json.dumps(doc, indent=4, ensure_ascii=False)
            soup.head.insert(1, new_tag)
        else:
            # set header message
            message = """
            No structured metadata on this page. <a href="{}">Try a different URL.</a> 
            """.format('//{}/_random.html?redirect'.format(self.host))

        # add uniform header
        html = BeautifulSoup("""
        <nav class="navbar navbar-expand-md navbar-dark mainBackDark fixed-top p-3" style="border-bottom: 8px #4A7D8F solid;">
            <a class="navbar-brand" href="https://discovery.biothings.io">
                <img src="https://discovery.biothings.io/static/img/dde-logo-o.svg" width="30" height="30" alt="logo">
            </a>
            <a id="logo" class="navbar-brand mainFont caps text-light" href="https://discovery.biothings.io">CTSA DATA DISCOVERY ENGINE</a>
            <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
                <span class="navbar-toggler-icon"></span>
            </button>

            <div class="collapse navbar-collapse justify-content-between" id="navbarSupportedContent">
                <small class="text-muted m-auto font-weight-bold alert alert-light">
                {}
                </small>
                <ul class="navbar-nav">
                <li class="nav-item"><a class="nav-link h-link" href="https://discovery.biothings.io/best-practices">Discovery Guide</a></li>
                <li class="nav-item"><a class="nav-link h-link" href="https://discovery.biothings.io/schema-playground">Schema Playground</a></li>
                </ul>
            </div>
        </nav>
        """.format(message), 'html.parser')
        soup.body.insert(2, html)
        soup.head.insert(2, BeautifulSoup("""
            <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css" integrity="sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO" crossorigin="anonymous">
        """, 'html.parser'))
        soup.head.insert(2, BeautifulSoup("""
            <link rel="stylesheet" href="https://discovery.biothings.io/static/css/styles.css">
        """, 'html.parser'))
        soup.head.insert(2, BeautifulSoup("""
            <style>
                body {
                    padding-top: 100px !important;
                }
            </style>
        """, 'html.parser'))
        self.finish(soup.prettify())


APP_LIST = [
    (r"/(GSE\d+)", NCBIGeoDatasetHandler),
    (r"/_random.html", NCBIRandomDatasetExplorer),
    (r"/(sitemap\d?.xml)", tornado.web.StaticFileHandler, {"path": "web"}),
    (tornado.routing.AnyMatches(), NCBIProxyHandler),
]
