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
sys.path.insert(0, './spiders')
from baby_spider import BabySpider


class InvalidArgument(Exception):
    pass


class FetchProxyFail(Exception):
    pass


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


def config_settings(spiderfeed, hotel_id, filenumber, proxies_on):
    filename = spiderfeed.zipfile_id + '-' + hotel_id + '.json'
    file = Path('output/' + filename)
    silent_remove(file)
    settings = get_project_settings()
    settings['FEED_URI'] = Path(file)
    settings['FEED_FORMAT'] = 'json'
    if proxies_on:
        settings['ROTATING_PROXY_LIST_PATH'] = Path('proxies/proxies{}.txt'.format(filenumber))
    return settings, file


def refresh_proxies(filenumber, proxies_on, iteration, iteration_frequency):
    if proxies_on:
        proxies = FetchProxies(filenumber)
        if iteration % 150 == 0 or iteration == 0:
            try:
                proxies.fetch()
            except FetchProxyFail:
                pass


def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


def name_this_file(bucket_name, sub_dir, prefix):
    bucket_list = get_bucket_file_list(bucket_name, sub_dir)
    prefix_files = [str(prefix_file) for prefix_file in bucket_list if prefix in str(prefix_file)]
    if len(prefix_files) == 0:
        suffix = '_0.json'
    else:
        regex = '(?<={}_).*?(?=.json)'.format(prefix)
        file_numbers = [int(re.search(regex, prefix_file).group(0))
                        for prefix_file in prefix_files if re.search(regex, prefix_file).group(0)]
        current_max = max(file_numbers)
        suffix = '_{}.json'.format(current_max+1)
    return prefix + suffix


def get_egg_name(local_path, prefix):
    path_dir_list = os.listdir(local_path)
    prefix_files = [prefix_file for prefix_file in path_dir_list if prefix in prefix_file]
    if len(prefix_files) == 0:
        suffix = '_0.json'
    else:
        regex = '(?<={}_).*?(?=.json)'.format(prefix)
        file_numbers = [int(re.search(regex, prefix_file).group(0))
                        for prefix_file in prefix_files if re.search(regex, prefix_file).group(0)]
        current_max = max(file_numbers)
        suffix = '_{}.json'.format(current_max + 1)
    return prefix + suffix


def drop_local_spider_egg(file_name, sub_dir_name, filenumber, json_data):
    local_path = 'output/' + sub_dir_name + '/'
    cwd = os.getcwd()
    if not does_dir_exist(os.path.join(cwd, local_path)):
        os.mkdir(os.path.join(cwd, local_path))
    prefix = str(filenumber) + '_' + file_name
    filename = get_egg_name(local_path, prefix)
    with open(Path(local_path + filename), 'w') as save_file:
        print('Dropping Egg: '.format(filename))
        save_file.write(json.dumps(json_data))


def does_dir_exist(path):
    return os.path.isdir(os.path.join(path))


def check_number_spider_eggs(match_str, sub_dir_name, option=None):
    local_path = 'output/' + sub_dir_name + '/'
    dir_list = os.listdir(local_path)
    files_containing_match_str = [file for file in dir_list if match_str in file]
    if option == 'list':
        return files_containing_match_str
    else:
        return len(files_containing_match_str)


def collect_spider_eggs(match_str, sub_dir_name):
    local_path = 'output/' + sub_dir_name + '/'
    file_list = check_number_spider_eggs(match_str, sub_dir_name, option='list')
    collection = {}
    for file in file_list:
        with open(Path(local_path + file), 'r') as open_file:
            data = json.loads(open_file.read())
        collection.update(data)
        silent_remove(Path(local_path + file))
    return collection


def spider_egg_handler(filenumber, egg_dict, egg_name, egg_save_folder,
                       dicts_per_egg, eggs_per_collection, google_bucket_save_dir, bucket):

    if len(egg_dict) >= dicts_per_egg:
        drop_local_spider_egg(egg_name, egg_save_folder, filenumber, egg_dict)
        egg_dict = {}
    num_spider_eggs = check_number_spider_eggs(egg_name, egg_save_folder)

    if num_spider_eggs >= eggs_per_collection:
        spider_eggs = collect_spider_eggs(egg_name, egg_save_folder)
        collection_name = name_this_file(bucket, google_bucket_save_dir, '{}_bot{}'.format(egg_save_folder,
                                                                                           filenumber))
        upload_json_blob(bucket, spider_eggs, google_bucket_save_dir + '/' + collection_name)
    return egg_dict


def main(filenumber, start_spider_index, bucket, proxies_on=False):
    bucket_sub_dir = 'ta-hotel/compiled/test'
    spiderfeed = SpiderFeeder(filenumber=filenumber, start_idx=start_spider_index)
    iteration = 0
    en_dict = {}
    other_dict = {}
    while spiderfeed.continue_feed:
        refresh_proxies(filenumber, proxies_on, iteration, 150)
        hotel_id = get_hotel_id(spiderfeed.current_url)
        if hotel_id:
            settings, file = config_settings(spiderfeed, hotel_id, filenumber, proxies_on)
            run_spider(BabySpider, settings, spiderfeed.current_url)
            print('{} Before English length: {}'.format(str(filenumber), str(len(en_dict))))
            print('{} Before Other length: {}'.format(str(filenumber), str(len(other_dict))))
            en_dict, other_dict = split_reviews_locally(str(file), en_dict, other_dict)
            silent_remove(file)
            en_gcp_bucket_save_dir = bucket_sub_dir + '/' + 'en_response'
            other_gcp_bucket_save_dir = bucket_sub_dir + '/' + 'other'
            en_dict = spider_egg_handler(filenumber, en_dict, 'en_reviews_egg',
                                         'english_reviews', 2, 3, en_gcp_bucket_save_dir, bucket)
            #other_dict = spider_egg_handler(filenumber, other_dict, 'other_reviews_egg',
            #                                'other_reviews', 10, 3, other_gcp_bucket_save_dir, bucket)
        else:
            print('Hotel ID not found in URL!!')
        spiderfeed.next_url()
        iteration += 1


if __name__ == "__main__":

    bucket_name = 'nlp_resources'
    parser = argparse.ArgumentParser()
    parser.add_argument("--filenumber", "-f", help="File number index of the file in the input directory to run "
                                                   "spidermother on")
    parser.add_argument("--spider_start_idx", "-s", help="Index in the xml the start feeder will begin ")
    args = parser.parse_args()

    if args.filenumber and args.spider_start_idx:
        main(int(args.filenumber), int(args.spider_start_idx), bucket_name)
    elif not args.spider_start_idx:
        main(int(args.filenumber), 0, bucket_name)
    else:
        raise InvalidArgument('Input a filenumber in the form: -f ## to run spidermother.py')


