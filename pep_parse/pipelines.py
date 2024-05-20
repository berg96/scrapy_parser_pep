# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import datetime as dt
from collections import defaultdict
from pathlib import Path

# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter

DATETIME_FORMAT = '%Y-%m-%d_%H-%M-%S'
BASE_DIR = Path(__file__).parent.parent
NAME_RESULTS_DIR = 'results'
HEADER_PEP_STATUSES = ('Статус', 'Количество')
FOOTER_PEP_STATUSES = 'Всего'


def get_results_dir(base_dir):
    return base_dir / NAME_RESULTS_DIR


class PepParsePipeline:
    def open_spider(self, spider):
        self.statuses = defaultdict(int)

    def process_item(self, item, spider):
        self.statuses[item['status']] += 1
        return item

    def close_spider(self, spider):
        now_formatted = dt.datetime.now().strftime(DATETIME_FORMAT)
        file_name = f'status_summary_{now_formatted}.csv'
        results_dir = get_results_dir(BASE_DIR)
        results_dir.mkdir(exist_ok=True)
        file_path = results_dir / file_name
        with open(file_path, 'w', encoding='utf-8') as f:
            writer = csv.writer(f, dialect=csv.unix_dialect)
            writer.writerows(
                [
                    HEADER_PEP_STATUSES,
                    *self.statuses.items(),
                    (FOOTER_PEP_STATUSES, sum(self.statuses.values())),
                ]
            )
