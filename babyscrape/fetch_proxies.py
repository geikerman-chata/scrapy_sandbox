from fp.fp import FreeProxy
import time
import os, errno
from pathlib import Path

class FetchProxies:
    def __init__(self, filenumber):
        self.filenumber = filenumber
        self.file_name = Path('proxies/proxies{}.txt'.format(self.filenumber))
        self.target_list_size = 10
        self.max_attempts = 25
        self.proxy_list = []

    def fetch(self):
        self.proxy_list = []
        attempts = 0
        while len(self.proxy_list) < self.target_list_size and attempts < self.max_attempts:
            tic = time.time()
            proxy = FreeProxy(country_id=['US', 'CA', 'GB', 'FR', 'JP'], rand=True).get()
            if proxy:
                proxy_truncated = proxy.replace('http://', '')
                if proxy_truncated not in self.proxy_list:
                    self.proxy_list.append(proxy_truncated)
                    toc = time.time()
                    print('Spider #: {} Proxy Added: {} on attempt {},'
                          ' time elapsed: {}'.format(self.filenumber, proxy_truncated, attempts, round(toc-tic, 3)))
            attempts += 1
        self.overwrite_file()

    def overwrite_file(self):
        self.silent_remove()
        with open(self.file_name, 'w') as writer:
            for proxy in self.proxy_list:
                writer.write("%s\n" % proxy)

    def silent_remove(self):
        try:
            os.remove(self.file_name)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise