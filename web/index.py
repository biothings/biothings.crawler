import json
import logging

import tornado.httpclient
import tornado.ioloop
import tornado.options
import tornado.routing
import tornado.web
from bs4 import BeautifulSoup
from scrapy.selector import Selector

from discovery.spiders.ncbi_geo import NCBIGeoSpider


async def transform(doc, url):

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
        "distribution": {
            "@type": "dataDownload",
            "contentUrl": url
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


class MainHandler(tornado.web.RequestHandler):

    async def get(self):

        url = 'https://www.ncbi.nlm.nih.gov' + self.request.uri
        http_client = tornado.httpclient.AsyncHTTPClient()
        response = await http_client.fetch(url, raise_error=False)

        self.set_status(response.code)
        self.set_header('Content-Type', response.headers.get('Content-Type'))

        if self.request.path == '/geo/query/acc.cgi':
            text = response.body.decode()
            soup = BeautifulSoup(text, 'html.parser')
            doc = NCBIGeoSpider().parse(Selector(text=text))
            doc = await transform(doc, url)
            new_tag = soup.new_tag('script', type="application/ld+json")
            new_tag.string = json.dumps(doc, indent=4, ensure_ascii=False)
            soup.head.insert(0, new_tag)
            self.finish(soup.prettify())
        else:
            self.finish(response.body)


if __name__ == "__main__":
    logging.getLogger().setLevel('DEBUG')
    application = tornado.web.Application([
        (tornado.routing.AnyMatches(), MainHandler),
    ], debug=True)
    application.listen(8080)
    tornado.ioloop.IOLoop.current().start()
