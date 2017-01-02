# -*- coding: utf-8 -*-
import scrapy
import bs4

class ActsSpider(scrapy.Spider):
    name = "acts"
    allowed_domains = ["www.gazette.gc.ca", "publications.gc.ca"]

    def start_requests(self):
        return [scrapy.Request(url='http://www.gazette.gc.ca/archives/part3-archives-partie3-eng.html',
                               callback=self.annual_link_callback)]

    def annual_link_callback(self, response):
        doc = bs4.BeautifulSoup(response.body, 'html.parser')
        content = doc.find('div', id='gazette_content')
        parts = content.find_all('div', **{'class': 'PublicationIndex'})
        for part in parts:
            year = part.find('strong').text
            link = part.find('a')['href']
            yield scrapy.Request(
                url=response.urljoin(link),
                callback=self.acts_per_year_callback,
                meta={'year': year}
            )

    def acts_per_year_callback(self, response):
        doc = bs4.BeautifulSoup(response.body, 'html.parser')
        content = doc.find('div', id='gazette_content')
        parts = content.find_all('div', **{'class': 'PublicationIndex'})
        for part in parts:
            volume = part.find('a')['title']
            link = part.find('a')['href']
            self.logger.info("Found document for volume {} at {}".format(volume, link))