# -*- coding: utf-8 -*-
import scrapy
import bs4
from google.cloud import storage
import os
import io
import re
from urllib import parse


BUCKET_NAME = 'cda-gazette-scraper'
REGS_FOLDER = 'regs'

class RegsSpider(scrapy.Spider):
    name = "regs"
    allowed_domains = ["www.gazette.gc.ca", "publications.gc.ca"]

    def start_requests(self):
        client = storage.Client()
        self.bucket = client.get_bucket(BUCKET_NAME)
        return [
            scrapy.Request(url='http://www.gazette.gc.ca/archives/part2-archives-partie2-eng.html',
                           callback=self.recent_regs_callback),
            scrapy.Request(url='http://www.gazette.gc.ca/archives/archives-eng.html',
                           callback=self.archived_regs_callback)
        ]

    def recent_regs_callback(self, response):
        doc = bs4.BeautifulSoup(response.body, 'html.parser')
        content = doc.find('div', id='gazette_content')
        content_list = content.find('ul')
        reg_years = content_list.find_all('a')
        for reg_year in reg_years:
            year = int(re.match('.*\((\d\d\d\d)\)', reg_year.text).group(1))
            url = response.urljoin(reg_year['href'])
            yield scrapy.Request(
                url=url,
                callback=self.regs_for_recent_year_callback,
                meta={'year': year}
            )

    def archived_regs_callback(self, response):
        document = bs4.BeautifulSoup(response.body, 'html.parser')
        table = document.find('table', id='archive-table')
        table_body = table.find('tbody')
        reg_years_col = table_body.find_all('ul', **{'class': 'list-bullet-none'})[1]
        reg_years = reg_years_col.find_all('a')

        for reg_year in reg_years:
            year_text = reg_year.find(text=True, recursive=False).extract()
            year = int(year_text.split('—')[1].strip())
            yield scrapy.Request(
                url=response.urljoin(reg_year['href']),
                callback=self.regs_for_archived_year_callback,
                meta={'year': year}
            )

    def regs_for_recent_year_callback(self, response):
        doc = bs4.BeautifulSoup(response.body, 'html.parser')
        table = doc.find('div', id='gazette_content')

        for child in table.div.find_all(['div', 'h2'], recursive=False):
            if child.name == 'h2' and child.text.startswith('Consolidate'):
                break
            elif child.name == 'div':
                pdf_link = child.find('span', **{'class': 'index-pdf'}).a
                year = response.meta['year']
                title = pdf_link['title']
                filename = self.extract_basename(pdf_link['href'])

                self.logger.info("Scraping gazette - Title: {}, Year: {}".format(title, year))
                if not self.check_if_file_in_storage(year, filename):
                    yield scrapy.FormRequest(
                        url=response.urljoin(pdf_link['href']),
                        formdata={'from_interstitial': '1'},
                        callback=self.download_file,
                        meta={'year': response.meta['year'], 'title': title, 'filename': filename}
                    )
                else:
                    self.logger.info('Already in storage - Title: {}, Year: {}'.format(title, year))

    def regs_for_archived_year_callback(self, response):
        doc = bs4.BeautifulSoup(response.body, 'html.parser')
        table = doc.find('div', id='gazette_content')

        for child in table.div.find_all(['div', 'h2'], recursive=False):
            if child.name == 'h2' and child.text.startswith('Consolidated'):
                break
            elif child.name == 'div':
                title = child.strong.text
                links = [link for link in child.find_all('a') if link.get('href', '').endswith('.pdf')]

                for link in links:
                    year = response.meta['year']
                    filename = self.extract_basename(link['href'])
                    self.logger.info("Scraping gazette - Title: {}, Year: {}".format(title, year))
                    if not self.check_if_file_in_storage(year, filename):
                        headers = {
                            'Referer': "http://publications.gc.ca/site/archivee-archived.html?url={}".format(link['href'])
                        }
                        yield scrapy.Request(
                            url=link['href'],
                            headers=headers,
                            callback=self.download_file,
                            meta={'year': year, 'title': title, 'filename': filename}
                        )
                    else:
                        self.logger.info('Already in storage - Title: {}, Year: {}'.format(title, year))
    @staticmethod
    def construct_path(year, filename):
        return '{}/{}/{}'.format(REGS_FOLDER, year, filename)

    @staticmethod
    def extract_basename(link):
        path = parse.urlparse(link).path
        return os.path.basename(path)

    def check_if_file_in_storage(self, year, filename):
        return self.bucket.get_blob(self.construct_path(year, filename)) is not None

    def download_file(self, response):
        if response.headers.to_unicode_dict()['Content-Type'] == 'application/pdf':
            self.logger.info('Downloading Gazette - Year: {}, Filename: {}'.format(response.meta['year'], response.meta['filename']))
            blob = storage.Blob(self.construct_path(response.meta['year'], response.meta['filename']), self.bucket)
            body = response.body
            body_bytes = io.BytesIO(response.body)
            blob.upload_from_file(body_bytes, size=len(body))
        self.logger.info("Downloading document for: Year {}, Title: {}".format(response.meta['year'], response.meta['title']))
