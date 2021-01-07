from scrapy.spiders import SitemapSpider


class SitemapperSpider(SitemapSpider):
    name ="sitemap"
    sitemap_urls = ['https://www.tripadvisor.ca/sitemap/2/en_CA/sitemap_en_CA_index.xml']
    #sitemap_follow = ['/sitemap_en_CA_index']

    def parse(self, response):
        yield {
            'url': response.url
        }