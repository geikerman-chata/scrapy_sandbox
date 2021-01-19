from pathlib import Path
import json
from google.cloud import storage
import copy


def load_contents(file):
    with open(file, 'r') as open_file:
        contents = open_file.read()
    return contents


def repack_data(full_dict, meta_key, key):
    data_pack = {}
    data_pack[meta_key + '-' + key] = {
        'meta_data': full_dict[meta_key],
        'review': full_dict[key]
    }
    return data_pack


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


def save_split_review(bucket_name, bucket_sub_dir, full_dict, meta_key, review_key):
    data_packet = repack_data(full_dict, meta_key, review_key)
    blob_filename = meta_key + '-' + review_key
    blob_save_path = str(Path(bucket_sub_dir + blob_filename))
    upload_json_blob(bucket_name, data_packet, blob_save_path)


def split_reviews(bucket_name, full_dict, bucket_sub_dir_in):
    meta_key_list = [key for key in full_dict.keys() if key[0] == 'g']
    if len(meta_key_list) != 1:
        pass
    else:
        meta_key = meta_key_list[0]
        for review_key in full_dict:
            bucket_sub_dir = copy.deepcopy(bucket_sub_dir_in)
            if review_key[0] == 'Y':
                bucket_sub_dir += 'response/'
                if full_dict[review_key]['review_language']:
                    lang = full_dict[review_key]['review_language']
                else:
                    lang = 'null'
                bucket_sub_dir += lang + '/'
                save_split_review(bucket_name, bucket_sub_dir, full_dict, meta_key, review_key)

            elif review_key[0] == 'N':
                bucket_sub_dir += 'no_response/'
                if full_dict[review_key]['review_language']:
                    lang = full_dict[review_key]['review_language']
                else:
                    lang = 'null'
                bucket_sub_dir += lang + '/'
                save_split_review(bucket_name, bucket_sub_dir, full_dict, meta_key, review_key)
            else:
                pass


def split_file_into_buckets(bucket_name, file, bucket_sub_dir='ta-crawler/'):
    contents = load_contents(file)
    if contents:
        json_data = json.loads(contents)
        if json_data:
            full_dict = json_data[0]
            split_reviews(bucket_name, full_dict, bucket_sub_dir)


