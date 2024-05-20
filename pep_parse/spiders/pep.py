import re

import scrapy

from pep_parse.items import PepParseItem


class PepSpider(scrapy.Spider):
    name = 'pep'
    allowed_domains = ['peps.python.org']
    start_urls = ['https://peps.python.org/']

    def parse(self, response):
        all_peps = response.css('a[href^="pep"]')
        for pep_link in all_peps:
            yield response.follow(pep_link, callback=self.parse_pep)

    def parse_pep(self, response):
        pattern = r'PEP (?P<version>\d+) – (?P<name>.*)'
        version, name = re.search(
            pattern, response.css('#pep-content h1::text').get()
        ).groups()
        yield PepParseItem(
            {
                'number': version,
                'name': name,
                'status': response.css(
                    'dt:contains("Status") + dd ::text'
                ).get()
            }
        )
