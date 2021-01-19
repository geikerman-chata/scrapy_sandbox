from google.cloud import storage
import json
from pathlib import Path
from split_output import split_reviews
from multiprocessing import Pool
from multiprocessing import get_context
import argparse

class InvalidArgument(Exception):
    pass

client = storage.Client()
bucket = client.bucket('nlp_resources')
print("Fetching Bucket List, takes a minute...")
full_list = list(bucket.list_blobs(prefix='ta-crawler/raw-output-3'))
print("Number of files in bucket: {}".format(len(full_list)))


done_list = {}
parser = argparse.ArgumentParser()
parser.add_argument("--workers", "-n", help="Number of workers to split up work")
parser.add_argument("--half", "-d", help="which half of the full list the workers will work on, 1 or 2")
args = parser.parse_args()

def blob_2_dict(blob):
    blob_dict = None
    bytes_data = blob.download_as_string()
    if bytes_data:
        json_data = json.loads(bytes_data)
        if json_data:
            blob_dict = json_data[0]
    return blob_dict

def divine_args(args, full_list):
    if args.half:
        halfway = int(round((float(len(full_list)))/2))
        if args.half == '1':
            task_list = full_list[:halfway]
        elif args.half == '2':
            task_list = full_list[halfway:]
        else:
            raise InvalidArgument('-d argument must be either 1 or 2')
    else:
        task_list = full_list

    if args.workers:
        try:
            num_workers = int(args.workers)
            if num_workers == 0:
                raise InvalidArgument('-n must be nonzero int')
        except InvalidArgument:
            raise InvalidArgument('-n must be nonzero int')
    else:
        num_workers = 1

    return task_list, num_workers


task_list, num_workers = divine_args(args, full_list)
bunch_increment = int(round(float(len(task_list))/num_workers))
task_chunks = [task_list[x:x+bunch_increment] for x in range(0, len(task_list), bunch_increment)]

bucket_name = 'nlp_resources'
bucket_sub_dir = 'ta-crawler/test/'
worker_number = 0


def split_google_blob(worker_number, task_chunks):
    for blob in task_chunks[worker_number]:
        blob_dict = blob_2_dict(blob)
        if blob_dict:
            split_reviews(bucket_name, blob_dict, bucket_sub_dir)








