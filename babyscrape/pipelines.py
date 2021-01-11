# -*- coding: utf-8 -*-


# Currently this is not activated, to activate add "BabyscrapyPipeline" to
#ITEM_PIPELINES setting in settings.py
#

from scrapy.exporters import JsonItemExporter
import json

class BabyscrapePipeline(object):

    file = None

    def open_spider(self, spider):
        self.file = open('test_in.json', 'wb')
        self.exporter = JsonItemExporter(self.file, indent=4)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item
