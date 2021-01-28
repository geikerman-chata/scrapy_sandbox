

# open_input folder

# read input files with specific name pattern, sum together and take average advancement
import os
import re
from pathlib import Path

def read_input_folder():
    path = Path(os.getcwd() + '/input')
    folder_contents = os.listdir(path)
    marker_list = [marker for marker in folder_contents if '_marker_' in marker]
    regex = '(?<=_marker_).*?(?=.txt)'
    markers = []
    bad_markers = []
    for n, marker_txt in enumerate(marker_list):
        start_idx = re.search(regex, marker_txt).group(0)
        try:
            with open(Path(path, marker_txt), 'r') as file:
                markers.append(n,(int(start_idx), file.read()))
        except:
            bad_markers.append(n, marker_txt)
    return markers, bad_markers

def remove_bad_markers():
    path = Path(os.getcwd() + '/input')
    folder_contents = os.listdir(path)
    marker_list = [marker for marker in folder_contents if '_marker_' in marker]
    for n, marker_txt in enumerate(marker_list):
        try:
            with open(Path(path, marker_txt), 'r') as file:
                if file.read() == '':
                    print('Deleting {}'.format(marker_txt))
                    os.remove(Path(path, marker_txt))
        except:
            pass

def sum_inputs(marker_list):
    sum_list =[]
    for i_pair in marker_list:
        sum_list.append(i_pair[1] - i_pair[0])
    total = sum(sum_list)
    return total, round(total/len(sum_list), 0)





