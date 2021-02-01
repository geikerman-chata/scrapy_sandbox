from google.cloud import storage
import json
from pathlib import Path


def blob_2_dict(blob):
    bytes_data = blob.download_as_string()
    if bytes_data:
        json_data = json.loads(bytes_data)
    else:
        json_data = None
    return json_data

def run_though_json(json_data, index):
    data_keys = json_data.keys()


    return index

client = storage.Client()
bucket = client.bucket('nlp_resources')
bucket_sub_dir_en = 'ta-hotel/compiled/en_response'
bucker_sub_dir_other = 'ta-hotel/compiled/other'

directories_to_index = [bucket_sub_dir_en, bucker_sub_dir_other]
index = dict()
for sub_dir in directories_to_index:
    full_list = list(bucket.list_blobs(prefix=sub_dir))
    for blob_file in full_list:
        file_contents = blob_2_dict(blob_file)
        if json_data:
