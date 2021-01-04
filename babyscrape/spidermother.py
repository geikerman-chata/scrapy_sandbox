import scrapy
from twisted.internet import reactor
import scrapy.crawler as crawler
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerRunner
from multiprocessing import Process, Queue
import sys
from scrapy.utils.project import get_project_settings
import re


url_queue = ['https://www.tripadvisor.com/Hotel_Review-g34439-d7940209-Reviews-The_Gates_Hotel_South_Beach_a_DoubleTree_by_Hilton.html',
             'https://www.tripadvisor.com/Hotel_Review-g32706-d249597-Reviews-Days_Inn_by_Wyndham_Merced_Yosemite_Area.html']


def get_hotel_id(url):
    found = re.search(r'[g]\d{4,}[-][d]\d{5,}', url)
    if found:
        return found.group()
    else:
        return None


def deploy_crawler(queue, spider, settings, url):
    try:
        runner = CrawlerProcess(settings)
        deferred = runner.crawl(spider, start_urls=[url])
        deferred.addBoth(lambda _: reactor.stop())
        reactor.run()
        queue.put(None)
    except Exception as e:
        queue.put(e)


def run_spider(spider, settings, url):
    queue = Queue()
    process = Process(target=deploy_crawler, args=(queue, spider, settings, url,))
    process.start()
    result = queue.get()
    process.join()

    if result is not None:
        raise result


def main():

    for base_url in url_queue:
        hotel_id = get_hotel_id(base_url)
        if hotel_id:
            settings = get_project_settings()
            settings['FEED_URI'] = 'output/' + hotel_id + '.json'
            settings['FEED_FORMAT'] = 'json'
            run_spider(BabySpider, settings, base_url)
        else:
            print('Hotel ID not found in URL!!')


if __name__ == "__main__":
    sys.path.insert(0, './spiders')
    from baby_spider import BabySpider
    main()


