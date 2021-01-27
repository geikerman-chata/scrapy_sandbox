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
from split_output import split_file_into_buckets
from split_output import split_reviews_locally
import json


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


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def upload_json_blob(bucket_name, json_data, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(
        data=json.dumps(json_data),
        content_type='application/json'
    )
    print('File uploaded to {}.'.format(
        destination_blob_name))


def get_bucket_file_list(bucket_name, sub_dir):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return list(bucket.list_blobs(prefix=sub_dir))


def name_this_file(bucket_name, sub_dir, prefix):
    bucket_list = get_bucket_file_list(bucket_name, sub_dir)
    prefix_files = [prefix_file for prefix_file in bucket_list if prefix in prefix_file]
    if len(prefix_files) == 0:
        suffix = '_0.json'
    else:
        regex = '(?<={}_).*?(?=.json)'.format(prefix)
        file_numbers = [int(re.search(regex, prefix_file).group(0))
                        for prefix_file in prefix_files if re.search(regex, prefix_file).group(0)]
        current_max = max(file_numbers)
        suffix = '_{}.json'.format(current_max+1)
    return prefix + suffix


def main(filenumber, start_spider_index, bucket_save, bucket, proxies_on=False):
    bucket_sub_dir = 'ta-hotel/compiled'
    iteration = 0
    spiderfeed = SpiderFeeder(filenumber=filenumber, start_idx=start_spider_index)
    proxies = FetchProxies(filenumber)
    en_dict = {}
    other_dict = {}
    if proxies_on:
        try:
            proxies.fetch()
        except FetchProxyFail:
            raise FetchProxyFail('Proxy List could not be populated on init')

    while spiderfeed.continue_feed :

        if proxies_on:
            if iteration % 150 == 0 and iteration != 0:
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
            if proxies_on:
                settings['ROTATING_PROXY_LIST_PATH'] = Path('proxies/proxies{}.txt'.format(filenumber))

            if bucket_save:
                run_spider(BabySpider, settings, spiderfeed.current_url)
                #upload_blob(bucket, str(file), str(Path(bucket_sub_dir_raw + filename)))
                #split_file_into_buckets(bucket, str(file))
                print('{} Before English length: {}'.format(str(filenumber), str(len(en_dict))))
                print('{} Before Other length: {}'.format(str(filenumber), str(len(other_dict))))
                en_dict, other_dict = split_reviews_locally(str(file), en_dict, other_dict)
                os.remove(file)
                if len(en_dict) >= 50000:
                    sub_sub_dir = bucket_sub_dir + '/' + 'en_response'
                    file_name = name_this_file(bucket, sub_sub_dir, 'en_reviews_bot_{}'.format(filenumber))
                    upload_json_blob(bucket, en_dict, sub_sub_dir + '/' + file_name)
                    en_dict = {}
                if len(other_dict) >= 50000:
                    sub_sub_dir = bucket_sub_dir + '/' + 'other'
                    file_name = name_this_file(bucket, sub_sub_dir, 'other_reviews_bot_{}'.format(filenumber))
                    upload_json_blob(bucket, other_dict, sub_sub_dir + '/' + file_name)
                    other_dict = {}
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
    bucket = 'nlp_resources'
    parser = argparse.ArgumentParser()
    parser.add_argument("--filenumber", "-f", help="File number index of the file in the input directory to run "
                                                   "spidermother on")
    parser.add_argument("--spider_start_idx", "-s", help="Index in the xml the start feeder will begin ")
    parser.add_argument("--bucket_save", "-b", help="Save to default bucket path on google cloud: {}".format(bucket))
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


