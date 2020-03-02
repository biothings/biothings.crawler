from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

process = CrawlerProcess(get_project_settings())

# process.crawl('figshare')
# process.crawl('harvard')
# process.crawl('ncbi_geo')
# process.crawl('omicsdi')
# process.crawl('zenodo')
# process.crawl('nyu')
# process.crawl('immport')
# process.crawl('harvard_tracing')
process.crawl('figshare_api')
process.start()
