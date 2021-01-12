import os
from bs4 import BeautifulSoup
import gzip
import linecache

class FileError(Exception):
    pass

class InputError(Exception):
    pass

class SpiderFeeder():
    def __init__(self, filenumber, start_idx=0, lowram=True, unzip=False,):
        self.path = os.path.join(os.getcwd(), 'input')
        self.zip_files = [file for file in os.listdir(self.path) if '.gz' in file]
        self.lowram = lowram
        self.unzip = unzip
        self.url_list = []
        self.current_url = None
        self.start_index = start_idx
        self.continue_feed = True

        if self.zip_files:
            self.current_file = self.zip_files[filenumber]
            self.url_txt_file_name = self.current_file.replace('.xml.gz', '_url.txt')
        else:
            raise FileError(
                'No zip files in folder, run download_hotel_xml.py to populate input folder with zip files to open'
                            )
        self.zipfile_id = self.current_file.split('-')[-1].replace('.xml.gz', '')
        self.marker_file_name = self.current_file.replace('.xml.gz', '_marker_{}.txt'.format(self.start_index))

        if not self.does_file_exist(self.marker_file_name):
            self.marker = self.start_index
            self.update_marker_file(self.marker)

        else:
            self.marker = self.read_marker_file()

        if not self.does_file_exist(self.url_txt_file_name) or unzip:
            print('Unzipping URL list: {}'.format(self.current_file))
            self.url_list = self.unzip_url()
            self.url_list_len = len(self.url_list)
            self.current_url = self.url_at_marker()
            self.dump_url_list()
            del self.url_list
        else:
            self.current_url = self.read_url_at_marker()
            self.url_list_len = self.read_num_lines(self.url_txt_file_name)

        if self.marker > self.url_list_len:
            raise InputError('Start index {} must be less than the length of url list {}'.format(self.marker,
                                                                                                 self.url_list_len))

    def does_file_exist(self, file):
        return os.path.isfile(os.path.join(self.path, file))

    def unzip_url(self):
        with gzip.open(os.path.join(self.path, self.current_file)) as readfile:
            contents = readfile.read()
        xml_soup = BeautifulSoup(contents, "html.parser")
        return [tagged.text for tagged in xml_soup.find_all('loc')]

    def update_marker_file(self, index):
        with open(os.path.join(self.path, self.marker_file_name), 'w') as writer:
            writer.write(str(index))

    def read_marker_file(self):
        with open(os.path.join(self.path, self.marker_file_name), 'r') as reader:
            return int(reader.read())

    def url_at_marker(self):
        return self.url_list[self.marker]

    def dump_url_list(self):
        with open(os.path.join(self.path, self.url_txt_file_name), 'w') as writer:
            for row in self.url_list:
                writer.write(str(row) + '\n')

    def read_num_lines(self, file):
        return sum(1 for line in open(os.path.join(self.path, file)))

    def read_url_at_marker(self):
        return linecache.getline(os.path.join(self.path, self.url_txt_file_name), self.marker + 1)

    def read_file_at_line(self, file, line):
        return linecache.getline(os.path.join(self.path, file), line + 1)

    def next_url(self):
        if self.marker == self.url_list_len - 1:
            self.current_url = None  #Will crash spidermother if end of xml is reached
        else:
            self.marker += 1
            marker = self.marker
            if not self.lowram:
                self.update_marker_file(marker)
                self.current_url = self.url_at_marker()
            else:
                self.update_marker_file(marker)
                self.current_url = self.read_url_at_marker()

