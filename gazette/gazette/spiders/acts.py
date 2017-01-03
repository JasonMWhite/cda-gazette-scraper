# -*- coding: utf-8 -*-
import scrapy
import bs4
from google.cloud import storage
import os
import io


BUCKET_NAME = 'cda-gazette-scraper'
ACTS_FOLDER = 'acts'

class ActsSpider(scrapy.Spider):
    name = "acts"
    allowed_domains = ["www.gazette.gc.ca", "publications.gc.ca"]

    def start_requests(self):
        client = storage.Client()
        self.bucket = client.get_bucket(BUCKET_NAME)
        return [scrapy.Request(url='http://www.gazette.gc.ca/archives/part3-archives-partie3-eng.html',
                               callback=self.annual_link_callback)]

    def annual_link_callback(self, response):
        doc = bs4.BeautifulSoup(response.body, 'html.parser')
        content = doc.find('div', id='gazette_content')
        parts = content.find_all('div', **{'class': 'PublicationIndex'})
        for part in parts:
            year = int(part.find('strong').text)
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
            year = response.meta['year']
            filename = os.path.basename(link)
            self.logger.info("Scraping gazette - Volume: {}, Year: {}".format(volume, link))
            if not self.check_if_file_in_storage(year, filename):
                if year <= 2010:
                    headers = {
                        'Referer': 'http://publications.gc.ca/site/archivee-archived.html?url={}'.format(link),
                    }
                    yield scrapy.Request(
                        url=response.urljoin(link),
                        headers=headers,
                        callback=self.download_file,
                        meta={'year': year, 'filename': filename}
                    )
                else:
                    req = scrapy.FormRequest(
                        url=response.urljoin(link),
                        formdata={'from_interstitial': '1'},
                        callback=self.download_file,
                        meta={'year': year, 'filename': filename}
                    )
                    yield req
            else:
                self.logger.info('Already in storage - Volume: {}, Year: {}'.format(volume, year))

    @staticmethod
    def construct_path(year, filename):
        return '{}/{}/{}'.format(ACTS_FOLDER, year, filename)

    def check_if_file_in_storage(self, year, filename):
        return self.bucket.get_blob(self.construct_path(year, filename)) is not None

    def download_file(self, response):
        if response.headers.to_unicode_dict()['Content-Type'] == 'application/pdf':
            self.logger.info('Downloading Gazette - Year: {}, Filename: {}'.format(response.meta['year'], response.meta['filename']))
            blob = storage.Blob(self.construct_path(response.meta['year'], response.meta['filename']), self.bucket)
            body = response.body
            body_bytes = io.BytesIO(response.body)
            blob.upload_from_file(body_bytes, size=len(body))
        else:
            self.logger.info('Download Failed! Year: {}, Filename: {}'.format(response.meta['year'], response.meta['filename']))
