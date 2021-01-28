from google.cloud import storage
import json
from pathlib import Path
from split_output import split_reviews
from datetime import datetime

import argparse
from langdetect import detect

class InvalidArgument(Exception):
    pass

def blob_2_dict(blob):
    bytes_data = blob.download_as_string()
    if bytes_data:
        json_data = json.loads(bytes_data)
    return json_data

def check_language(review):
    try:
        lang = detect(review)
    except:
        lang = None
    return lang

def rollup_chunk(rolling_reviews, bad_reviews, task_chunk, save_dir, chunk_name, worker_id, previously_seen):
    for number, review in enumerate(task_chunk):
        print("Worker {} Opening file_number: {}".format(worker_id, number))

        try:
            review_dict = blob_2_dict(review)
            data_key_list = [key for key in review_dict.keys() if key[0] == 'g']
            if len(data_key_list) > 0:
                data_key = data_key_list[0]
                if data_key not in previously_seen:
                    previously_seen.update(data_key)
                    print("Worker {} New File!: {}".format(worker_id, data_key))
                    if "review" in review_dict[data_key].keys():
                        if 'review_text' in review_dict[data_key]['review']:
                            if len(review_dict[data_key]['review']['review_text']) > 760 \
                                    and review_dict[data_key]['review']['review_text'][-1] == '…':
                                bad_reviews[data_key] = "truncated"
                            elif check_language(review_dict[data_key]['review']['review_response']) != 'en':
                                bad_reviews[data_key] = "response not english"
                            else:
                                rolling_reviews[data_key] = review_dict[data_key]
                else:
                    print("Worker {} Seen before: {}".format(worker_id, data_key))

        except:
            print('Failed: {}'.format(number))

    upload_json_blob('nlp_resources', rolling_reviews, save_dir + chunk_name)
    upload_json_blob('nlp_resources', bad_reviews, '{}bad_reviews{}.json'.format(save_dir, worker_id))


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


def main():
    client = storage.Client()
    bucket = client.bucket('nlp_resources')
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk_start", "-s", help="chunk start")
    parser.add_argument("--chunk_size", "-c", help="worker id")
    parser.add_argument("--subdir", "-p", help="subdirectory to roll up")
    args = parser.parse_args()
    now = datetime.now()
    date = now.strftime("%d-%m-%Y")

 #   args.chunk_start = 1
 #   args.chunk_size = 250000
 #   args.subdir = 'ta-hotel/response'


    if args.subdir and args.chunk_size and args.chunk_start:
        print("Fetching Google Bucket list, takes a minute...")
        subdir = args.subdir
        full_list = list(bucket.list_blobs(prefix=args.subdir))
        if len(full_list) == 0:
            raise InvalidArgument("given subdirectory is empty")
        print("Number of files in bucket: {}".format(len(full_list)))
        subdir_top = subdir.split('/')[0]
        save_dir = '{}/compiled/'.format(subdir_top)
        bunch_increment = int(args.chunk_size)
        task_chunks = [full_list[x:x + bunch_increment] for x in range(0, len(full_list), bunch_increment)]
        print('There are a total of {} chunks when bunch increment is {}'.format(len(task_chunks), bunch_increment))
        filename = 'en_reviews-{}.json'.format(date)
        task = task_chunks[int(args.chunk_start)]
        worker_id = args.chunk_start
        file_list = list(bucket.list_blobs(prefix=save_dir))
        previously_seen_files = [name for name in file_list if 'en_reviews-24-01-2021.json' in str(name)
                                 or 'bad_reviews.json' in str(name)]

        previously_seen = set()
        for old_file in previously_seen_files:
            print('Loading set of previously seen files')
            old_file_dict = blob_2_dict(old_file)
            old_file_dict_index = set(old_file_dict.keys())
            previously_seen.update(old_file_dict_index)
        del old_file_dict_index
        del old_file_dict
        rolling_reviews = {}
        bad_reviews ={}
        rollup_chunk(rolling_reviews, bad_reviews, task, save_dir, filename, worker_id, previously_seen)


if __name__ == "__main__":
    main()
