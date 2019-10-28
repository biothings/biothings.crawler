# Biothings Crawler

Embedding structured metadata to provide explicit clues about the meaning of a web page has been increasingly embraced to facilitate the discovery of information. While a search engine could use structured data to enable special search result features and enhancements, researchers could navigate knowledge databases without learning their distinctive access interfaces.

When websites already have structured metadata embedded, extracting all the metadata commonly just require crawling, which is systematically browsing the websites for indexing information of interest. We can find all pages to browse in a website through URL patterns, following links from a starting page or iterating through a sitemap, a list of pages provided by the website.

There are also many websites that do not have contents described by structured metadata. For these sites, through web-scraping, which is typically harvesting data by HTML elements, we can handle these pages and build the structured metadata. Furthermore, we can even inject the metadata and serve the sites as if structured metadata are present from the view of other users and search engines.

When we have structured metadata, previously exclusively formatted data is now standardized to be interoperable for data discovery and analysis. Data can be aggregated according to certain criteria or indexed for full-text searching across databases. We crawled over 60,000 datasets hosted on Zenodo, Harvard Dataverse, NCBI Geo and Omicsdi, and extracted their metadata in schema.org standard. What we can conclude from the existing collection provide valuable insights in our effort to design future schemas to describe biological discoveries.
