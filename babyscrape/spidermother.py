import scrapy
from datetime import datetime
from twisted.internet import reactor
import scrapy.crawler as crawler
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerRunner
from multiprocessing import Process, Queue
import sys
from scrapy.utils.project import get_project_settings
import re
import os, errno
from spider_feeder import SpiderFeeder
from fetch_proxies import FetchProxies
import argparse
from pathlib import Path
from google.cloud import storage



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


def store_locally(filenumber, file):
    silent_remove(file)
    settings = get_project_settings()
    settings['FEED_URI'] = Path(file)
    settings['FEED_FORMAT'] = 'json'
    settings['ROTATING_PROXY_LIST_PATH'] = Path('proxies/proxies{}.txt'.format(filenumber))
    return settings


def upload_blob(bucket_name, source_file_name, destination_blob_name):
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)
  blob.upload_from_filename(source_file_name)
  print('File {} uploaded to {}.'.format(
      source_file_name,
      destination_blob_name))

def store_in_bucket(filenumber, file):

    silent_remove(file)
    settings = get_project_settings()
    settings['FEED_URI'] = Path(file)
    settings['FEED_FORMAT'] = 'json'
    settings['ROTATING_PROXY_LIST_PATH'] = Path('proxies/proxies{}.txt'.format(filenumber))
    return settings


def main(filenumber, start_spider_index, bucket_save, bucket):
    iteration = 0
    spiderfeed = SpiderFeeder(filenumber=filenumber, start_idx=start_spider_index)
    proxies = FetchProxies(filenumber)
    try:
        proxies.fetch()
    except FetchProxyFail:
        raise FetchProxyFail('Proxy List could not be populated on init')

    while spiderfeed.continue_feed:
        if iteration % 50 == 0 and iteration != 0:
            try:
                proxies.fetch()
            except FetchProxyFail:
                pass
        hotel_id = get_hotel_id(spiderfeed.current_url)

        if hotel_id:
            filename = spiderfeed.zipfile_id + '-' + hotel_id + '.json'
            file = Path('output/' + filename)
            silent_remove(file)
            settings = get_project_settings()
            settings['FEED_URI'] = Path(file)
            settings['FEED_FORMAT'] = 'json'
            settings['ROTATING_PROXY_LIST_PATH'] = Path('proxies/proxies{}.txt'.format(filenumber))
            if bucket_save:
                run_spider(BabySpider, settings, spiderfeed.current_url)
                upload_blob(bucket, file, filename)
                os.remove(file)
            else:
                run_spider(BabySpider, settings, spiderfeed.current_url)

        else:
            print('Hotel ID not found in URL!!')
        spiderfeed.next_url()
        iteration += 1


def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

class InvalidArgument(Exception):
    pass
class FetchProxyFail(Exception):
    pass

if __name__ == "__main__":
    sys.path.insert(0, './spiders')
    from baby_spider import BabySpider

    bucket = Path('nlp_resources/ta-crawler/raw_output')
    parser = argparse.ArgumentParser()
    parser.add_argument("--filenumber", "-f", help="File number index of the file in the input directory to run "
                                                   "spidermother on")
    parser.add_argument("--spider_start_idx", "-s", help="Index in the xml the start feeder will begin ")
    parser.add_argument("--bucket_save", "-b", help ="Save to default bucket path on google cloud: {}".format(bucket))
    args = parser.parse_args()

    if args.bucket_save:
        bucket_save = True
    else:
        bucket_save = False

    if args.filenumber and args.spider_start_idx:
        main(int(args.filenumber), int(args.spider_start_idx), bucket_save, bucket)
    elif not args.spider_start_idx:
        main(int(args.filenumber), 0, bucket_save, bucket)
    else:
        raise InvalidArgument('Input a filenumber in the form: -f ## to run spidermother.py')


