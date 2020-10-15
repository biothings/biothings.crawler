import time
import os
import uuid
import sys


os.chdir(os.path.abspath(os.path.dirname(__file__)))
# patch PATH so local venv is in PATH
bin_path = os.path.join(os.getcwd(), 'venv/bin')
os.environ['PATH'] += os.pathsep + bin_path

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from elasticsearch import Elasticsearch

from crawler.upload import uploaders

u = uuid.uuid1()

crawler_host = "api.outbreak.info"
uploader_host = "api.outbreak.info"
crawler_index = f"crawler_immport_covid_{u.hex}"
uploader_index = f"uploader_immport_covid_{u.hex}"
alias_name = "outbreak-resources-immport-covid"

es_crawler = Elasticsearch(crawler_host)
es_uploader = Elasticsearch(uploader_host)


# crawler uses env vars for this
os.environ['ES_INDEX'] =crawler_index
os.environ['ES_HOST'] = crawler_host

# crawl
process = CrawlerProcess(get_project_settings())
process.crawl('immport_covid')
process.start()

# FIXME: this is a hack to wait for reindex without forcing it
time.sleep(15)

# upload
uploader = uploaders['immport'](
    src_host=crawler_host,
    src_index=crawler_index,
    dest_host=uploader_host,
    dest_index=uploader_index
)
uploader.upload()

# assuming it hasn't crashed
es_crawler.indices.delete(crawler_index)

# alias update
if not es_uploader.indices.exists(alias_name):
    es_uploader.indices.put_alias(index=uploader_index, name=alias_name)
else:
    # if an index not alias exists, just let it crash
    actions = {
        "actions": [
            {"add": {"index": uploader_index, "alias": alias_name}}
        ]
    }
    rm_idx = [i_name for i_name in es_uploader.indices.get_alias(alias_name)]
    removes = [{
        "remove": {"index": index_name, "alias": alias_name}
        } for index_name in rm_idx
        ]
    actions["actions"].extend(removes)
    es_uploader.indices.update_aliases(actions)
    for rm_i in rm_idx:
        es_uploader.indices.delete(rm_i)
