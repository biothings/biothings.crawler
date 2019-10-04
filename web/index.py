import json
import logging

import tornado.httpclient
import tornado.ioloop
import tornado.options
import tornado.routing
import tornado.web
from bs4 import BeautifulSoup
from scrapy.selector import Selector
from tornado.options import define, options

from discovery.spiders.ncbi_geo import NCBIGeoSpider


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
    pmid = doc.get("Citation(s)")
    if pmid:

        # funders
        http_client = tornado.httpclient.AsyncHTTPClient()
        url = "https://www.ncbi.nlm.nih.gov/pubmed/" + pmid
        response = await http_client.fetch(url)
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
                    'identifier': identifier,
                } for funder, identifier in zip(funders, identifiers)
            ]

        # citation
        http_client = tornado.httpclient.AsyncHTTPClient()
        citation_url = 'https://www.ncbi.nlm.nih.gov/sites/PubmedCitation?id=' + pmid
        citation_response = await http_client.fetch(citation_url)
        citation_text = Selector(text=citation_response.body.decode()).xpath('string(/)').get()
        _doc['citation'] = citation_text.replace(u'\xa0', u' ')

    for key, value in doc.items():
        if key in mappings:
            if isinstance(mappings[key], str):
                _doc[mappings[key]] = value
            elif callable(mappings[key]):
                _doc.update(mappings[key](value))
            else:
                raise RuntimeError()

    return dict(sorted(_doc.items()))


class NCBIProxyHandler(tornado.web.RequestHandler):

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "https://discovery.biothings.io")
        self.set_header("Access-Control-Allow-Headers", "X-Requested-With")
        self.set_header('Access-Control-Allow-Methods', 'GET, OPTIONS')

    def options(self):
        self.set_status(204)
        self.finish()

    async def get(self):

        root = 'https://www.ncbi.nlm.nih.gov'
        url = root + self.request.uri
        http_client = tornado.httpclient.AsyncHTTPClient()
        response = await http_client.fetch(url, raise_error=False)

        self.set_status(response.code)
        self.set_header('Content-Type', response.headers.get('Content-Type'))
        self.finish(response.body)


class NCBIGeoDatasetHandler(tornado.web.RequestHandler):

    async def get(self, gse_id):

        url = 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' + gse_id
        http_client = tornado.httpclient.AsyncHTTPClient()
        response = await http_client.fetch(url)

        # add metadata
        text = response.body.decode()
        soup = BeautifulSoup(text, 'html.parser')
        doc = NCBIGeoSpider().parse(Selector(text=text))
        doc = await transform(doc, url, gse_id)
        new_tag = soup.new_tag('script', type="application/ld+json")
        new_tag.string = json.dumps(doc, indent=4, ensure_ascii=False)
        soup.head.insert(0, new_tag)

        # add header
        header = soup.new_tag('p', align='center')
        original_link = soup.new_tag('a', href=url)
        original_link.string = gse_id
        header.append('This page adds structured')
        schema_link = soup.new_tag('a', href='http://schema.org/Dataset')
        schema_link.string = 'schema.org/Dataset'
        header.append(schema_link)
        header.append('metadata to this original GEO data series page for')
        header.append(original_link)
        soup.body.insert(0, header)

        # resource path redirection
        host = self.request.host.split(':')[0]
        soup.head.insert(0, soup.new_tag(
            'base', href='//{}:{}/geo/query/'.format(host, options.redirect)))

        self.finish(soup.prettify())


if __name__ == "__main__":
    define("port", default=8080, help="port to listen on")
    define("debug", default=True, help="enable debug logging and autoreload")
    define("redirect", default=8080, help="port to load resources")
    options.parse_command_line()
    if options.debug:
        logging.getLogger().setLevel('DEBUG')
    application = tornado.web.Application([
        (r"/(GSE\d+)", NCBIGeoDatasetHandler),
        (tornado.routing.AnyMatches(), NCBIProxyHandler),
    ], debug=options.debug)
    application.listen(options.port)
    tornado.ioloop.IOLoop.current().start()
