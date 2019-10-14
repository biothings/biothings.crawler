
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

xml_file = "sitemap.xml"
client = Elasticsearch('su07:9199')
index = 'ncbi_geo'

search = Search(using=client, index=index)
ids = sorted([doc.meta.id for doc in search.scan()], key=lambda esid: int(esid[3:]))

urlset = ET.Element('urlset', xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
for id in ids:
    url = ET.SubElement(urlset, 'url')
    loc = ET.SubElement(url, 'loc')
    loc.text = "https://discovery.biothings.io/metadata/geo/" + id

ET.ElementTree(urlset).write(xml_file, encoding="UTF-8", xml_declaration=True)

bs = BeautifulSoup(open(xml_file), 'xml')
with open(xml_file, 'w', encoding='utf-8') as file:
    file.write(bs.prettify())
