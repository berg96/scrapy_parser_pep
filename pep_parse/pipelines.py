# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import datetime as dt
import os
from collections import defaultdict

from pep_parse.settings import BASE_DIR, RESULTS_DIR, NAME_RESULTS_DIR

# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter

DATETIME_FORMAT = '%Y-%m-%d_%H-%M-%S'
HEADER_PEP_STATUSES = ('Статус', 'Количество')
FOOTER_PEP_STATUSES = 'Всего'


class PepParsePipeline:
    def __init__(self):
        RESULTS_DIR.mkdir(exist_ok=True)

    def open_spider(self, spider):
        self.statuses = defaultdict(int)

    def process_item(self, item, spider):
        self.statuses[item['status']] += 1
        return item

    def close_spider(self, spider):
        now_formatted = dt.datetime.now().strftime(DATETIME_FORMAT)
        file_name = f'{NAME_RESULTS_DIR}/status_summary_{now_formatted}.csv'
        file_path = os.path.join(BASE_DIR, file_name)
        with open(file_path, 'w', encoding='utf-8') as f:
            writer = csv.writer(
                f, dialect=csv.unix_dialect, quoting=csv.QUOTE_MINIMAL
            )
            writer.writerows(
                [
                    HEADER_PEP_STATUSES,
                    *self.statuses.items(),
                    (FOOTER_PEP_STATUSES, sum(self.statuses.values())),
                ]
            )
