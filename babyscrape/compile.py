from google.cloud import storage
import json
from pathlib import Path

client = storage.Client()
bucket = client.bucket('nlp_resources')


def repack_data(blob_dict, meta_key, key):
    data_pack = {}
    data_pack[meta_key + '-' + key] = {
        'meta_data': blob_dict[meta_key],
        'review': blob_dict[key]
    }
    return data_pack


def save_response(bucket_sub_dir, json_data, filename):
    bucket_name = 'nlp_resources'
    upload_json_blob(bucket_name, json_data, str(Path(bucket_sub_dir + filename)))


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


def isolate_english_responses(blob_dict):
    en_reviews_w_response = []
    en_reviews_no_response = []
    meta_key_list = [key for key in blob_dict.keys() if key[0] =='g']
    if len(meta_key_list) == 1:
        meta_key=meta_key_list[0]
        for key in blob_dict:
            if key[0] == 'Y':
                if blob_dict[key]['review_language'] == 'en':
                    data_pack = repack_data(blob_dict, meta_key, key)
                    en_reviews_w_response.append(data_pack)
            elif key[0] == 'N':
                if blob_dict[key]['review_language'] == 'en':
                    data_pack = repack_data(blob_dict, meta_key, key)
                    en_reviews_no_response.append(data_pack)
            else:
                pass
        return en_reviews_w_response, en_reviews_no_response
    else:
        return None, None


for blob in bucket.list_blobs(prefix='ta-crawler/raw-output'):
    bytes_data = blob.download_as_string()
    if bytes_data:
        json_data = json.loads(bytes_data)
        if json_data:
            blob_dict = json_data[0]
            en_reviews_w_response, en_reviews_no_response = isolate_english_responses(blob_dict)
            if en_reviews_w_response:
                for review in en_reviews_w_response:
                    filename = list(review.keys())[0]
                    bucket_sub_dir = 'ta-crawler/EN_response/'
                    save_response(bucket_sub_dir, review, filename)
            if en_reviews_no_response:
                for no_review in en_reviews_no_response:
                    filename = list(no_review.keys())[0]
                    bucket_sub_dir = 'ta-crawler/EN_no_response/'
                    save_response(bucket_sub_dir, no_review, filename)






