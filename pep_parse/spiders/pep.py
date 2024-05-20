import re

import scrapy

from pep_parse.items import PepParseItem


class PepSpider(scrapy.Spider):
    name = 'pep'
    allowed_domains = ['peps.python.org']
    start_urls = [f'https://{domain}/' for domain in allowed_domains]

    def parse(self, response):
        for pep_link in response.css('a[href^="pep"]'):
            yield response.follow(pep_link, callback=self.parse_pep)

    def parse_pep(self, response):
        pattern = r'PEP (?P<version>\d+) – (?P<name>.*)'
        version, name = re.search(
            pattern, response.css('#pep-content h1::text').get()
        ).groups()
        yield PepParseItem(
            number=version,
            name=name,
            status=response.css('dt:contains("Status") + dd ::text').get()
        )
