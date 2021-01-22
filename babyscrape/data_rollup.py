from google.cloud import storage
import json
from pathlib import Path
from split_output import split_reviews
from multiprocessing import Pool, get_context
import argparse


class InvalidArgument(Exception):
    pass


def blob_2_dict(blob):
    bytes_data = blob.download_as_string()
    if bytes_data:
        json_data = json.loads(bytes_data)
    return json_data

def rollup_chunk(task_chunk, chunk_name, bad_reviews):
    rolling_reviews = {}
    for file_number, review in enumerate(task_chunk):
        print("Opening file_number: {}".format(file_number))
        review_dict = blob_2_dict(review)
        data_key_list = [key for key in review_dict.keys() if key[0] == 'g']
        if len(data_key_list) > 0:
            data_key = data_key_list[0]
            if "review" in review_dict[data_key].keys():
                if 'review_text' in review_dict[data_key]['review']:
                    if len(review_dict[data_key]['review']['review_text']) < 780 \
                            and review_dict[data_key]['review']['review_text'][-1] != '…':
                        rolling_reviews[data_key] = review_dict[data_key]
                    else:
                        bad_reviews[data_key] = review_dict

    upload_json_blob('nlp_resources', rolling_reviews, 'ta-crawler/{}.json'.format(chunk_name))
    del rolling_reviews
    return bad_reviews

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
    print("Fetching Google Bucket list, takes a minute...")
    full_list = list(bucket.list_blobs(prefix='ta-crawler/response/en'))
    print("Number of files in bucket: {}".format(len(full_list)))
    bad_reviews = {}
    bunch_increment = 250000
    task_chunks = [full_list[x:x + bunch_increment] for x in range(0, len(full_list), bunch_increment)]
    del full_list

    for file_number, chunk in enumerate(task_chunks):
        chunk_name = 'en_reviews_{}.json'.format(str(file_number+1))
        bad_reviews = rollup_chunk(chunk, chunk_name, bad_reviews)
    upload_json_blob('nlp_resources', bad_reviews, 'ta-crawler/{}.json'.format('truncated_en_reviews_list.json'))

if __name__ == "__main__":
    main()
