from google.cloud import storage
import json
from pathlib import Path
from split_output import split_file_into_buckets
from multiprocessing import Pool
from multiprocessing import get_context
import argparse

class InvalidArgument(Exception):
    pass

client = storage.Client()
bucket = client.bucket('nlp_resources')
print("Fetching Bucket List...takes a minute")
full_list = list(bucket.list_blobs(prefix='ta-crawler/raw-output-3'))
print("Number of files in bucket".format(len(full_list)))


done_list = {}
parser = argparse.ArgumentParser()
parser.add_argument("--workers", "-n", help="Number of workers to split up work")
parser.add_argument("--half", "-d", help="which half of the full list the workers will work on, 1 or 2")
args = parser.parse_args()


def divine_args(args, full_list):
    if args.half:
        halfway = int(round((float(len(full_list)))/2))
        if args.half == 1:
            task_list = full_list[:halfway]
        elif args.half == 2:
            task_list = full_list[halfway:]
        else:
            raise InvalidArgument('-h argument must be either 1 or 2')
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
chunks = [task_list[x:x+bunch_increment] for x in range(0, len(task_list), bunch_increment)]






#for blob in bucket.list_blobs(prefix='ta-crawler/raw-output-3'):
#    bytes_data = blob.download_as_string()
#    if bytes_data:
#        json_data = json.loads(bytes_data)
#        if json_data:
#            blob_dict = json_data[0]







