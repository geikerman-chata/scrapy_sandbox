import os
from multiprocessing import Pool
from pathlib import Path
from multiprocessing import get_context
import argparse


def make_processes(spider_range_idx, start_index, bucket):
    if start_index == 0:
        start_string = ''
    else:
        start_string = ' -s ' + str(start_index)
    if bucket:
        start_string = start_string + ' -b'
    processes = ()
    for process_number in range(spider_range_idx[0], spider_range_idx[1]):
        processes = processes + ('spidermother.py -f {}{}'.format(process_number, start_string),)
    return processes


def run_process(process):
    os.system('python3 {}'.format(process))


class InvalidArgument(Exception):
    pass


def main(range_idx, start_idx=0, bucket=True):
    number_of_processes = len(range(range_idx[0], range_idx[1]))
    processes = make_processes(range_idx, start_idx, bucket)
    pool = Pool(processes=number_of_processes)
    pool.map(run_process, processes)


if __name__ == "__main__":
    input_files = os.listdir(Path('input'))
    xml_files = [xml for xml in input_files if '.xml.' in xml]
    max_spiders = len(xml_files)
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_index", "-i", help="Which index in the xml to start on")
    parser.add_argument("--start", "-s", help="Start target process file number")
    parser.add_argument("--finish", "-f", help="End target process file number")
    args = parser.parse_args()
    bucket = True
    if args.start and args.finish:
        range_idx = (int(args.start), int(args.finish), bucket)
    elif args.start and not args.finish:
        range_idx = (int(args.start), int(args.start), bucket)
    elif not args.finish and args.finish:
        range_idx = (int(args.finish), int(args.finish), bucket)
    else:
        range_idx = None

    if range_idx:
        if range_idx[1] > max_spiders - 1:
            range_idx = (range_idx[0], max_spiders - 1)
        if args.start_index:
            main(range_idx=range_idx, start_idx=args.start_index)
        else:
            main(range_idx=range_idx)
    else:
        raise InvalidArgument('spider_control.py takes -s and -f as arguments, must be integers < '.format(max_spiders - 1))





