from pathlib import Path
import json
from google.cloud import storage
import copy
import time
from langdetect import detect
import fcntl
import os

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
    attempts = 0
    data_packet = repack_data(full_dict, meta_key, review_key)
    blob_filename = meta_key + '-' + review_key
    blob_save_path = str(Path(bucket_sub_dir + blob_filename))
    while attempts <= 2:
        try:
            upload_json_blob(bucket_name, data_packet, blob_save_path)
            break
        except:
            print('Upload Failed, retrying: {} / 2'.format(attempts + 1))
            time.sleep(1)
            attempts += 1


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


def check_language(review):
    try:
        lang = detect(review)
    except:
        lang = None
    return lang


def get_languages(review):
    if 'review_language' in review.keys():
        lang_review = review['review_language']
    else:
        lang_review = None
    if 'response_language' in review.keys():
        lang_response = review['response_language']
    elif 'review_response' in review.keys():
        lang_response = check_language(review['review_response'])
    else:
        lang_response = None

    return lang_review, lang_response


def is_short(review):
    if len(review['review_text']) > 760 and review['review_text'][-1] == '…':
        return True
    else:
        return False


def update_local_dict(bucket_name, save_dir, filename, new_json_list, limit, destination_blob_name):
    file_path = Path(save_dir + filename)
    if not os.path.isfile(file_path):
        zero_file(file_path)

    file_info = os.stat(file_path)
    file_size = print(file_info.st_size)

    if file_size >= limit:
        upload_file_as_blob(bucket_name, filename, destination_blob_name)
        zero_file(file_path)

    with open(file_path, "a+") as locked_file:
        fcntl.flock(locked_file, fcntl.LOCK_EX)
        for data_pack in new_json_list:
            json.dump(data_pack, locked_file, indent=4)
        fcntl.flock(locked_file, fcntl.LOCK_UN)


def zero_file(file_path):
    with open(file_path, "w") as locked_file:
        fcntl.flock(locked_file, fcntl.LOCK_EX)
        json.dump({}, locked_file)
        fcntl.flock(locked_file, fcntl.LOCK_UN)


def upload_file_as_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def check_contents(contents):
    full_dict = None
    if contents:
        json_data = json.loads(contents)
        if json_data:
            full_dict = json_data[0]
    return full_dict


def split_reviews_locally(file, en_dict, other_dict):
    contents = load_contents(file)
    full_dict = check_contents(contents)
    if full_dict:
        meta_key_list = [key for key in full_dict.keys() if key[0] == 'g']
        if len(meta_key_list) != 1:
            pass
        else:
            meta_key = meta_key_list[0]
            for review_key in full_dict:
                response = review_key[0] #either 'Y' or 'N'
                if response == 'Y' or response == 'N':
                    review = full_dict[review_key]
                    lang_review, lang_response = get_languages(review)
                    short = is_short(review)
                    if lang_response == 'en' and lang_review == 'en' and response == 'Y' and not short:
                        en_dict.update(review)
                    elif not short:
                        other_dict.update(review)
                    else:
                        pass
    return en_dict, other_dict


def split_file_into_buckets(bucket_name, file, bucket_sub_dir='ta-hotel/'):
    contents = load_contents(file)
    if contents:
        json_data = json.loads(contents)
        if json_data:
            full_dict = json_data[0]
            split_reviews(bucket_name, full_dict, bucket_sub_dir)


