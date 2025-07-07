import json
import logging
import math
import os
import re
from pathlib import Path
from urllib import parse
import csv
import time
import random

import requests
from bs4 import BeautifulSoup
from degree72.core.base_class import BaseLog
from degree72.implementations.blocked_checkers.request_blocked_checker import RequestBlockedChecker
from degree72.implementations.daos.csv_dao import CsvDao
from degree72.implementations.downloaders.request_downloader import RequestDownloader
from degree72.implementations.dump_managers.local_dump_manager import LocalDumpManager
from degree72.utils.http.header_utils import parse_fiddler_headers, parse_chrome_headers
from biorxiv.common_params import headers_category_raw
from degree72.implementations.actions.http.request_action import RequestAction

class BioScraper(RequestAction):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger("BiorxivScraper")
        self.session = requests.Session()
        self.blocked_checker = RequestBlockedChecker()
        self.logger.set_log_level('INFO')
        from degree72.implementations.downloaders.curl_cffi_downloader import CurlCffiDownloader
        self.downloader = CurlCffiDownloader(
            blocked_checker=self.blocked_checker,
            proxy_point=self.PROXY_LOCAL_CLASH
        )

        storage_dir = Path(__file__).parent / "biorxiv_data"
        self.dumper = LocalDumpManager(
            project="biorxiv_scraper",
            base_dir=str(storage_dir)
        )
        self.dao = CsvDao()
        self.headers = parse_chrome_headers(headers_raw=headers_category_raw)

    def sanitize_filename(self, filename):
        """移除文件名中的非法字符"""
        # 替换非法字符为下划线
        filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
        # 移除连续的多个下划线
        filename = re.sub(r'_+', '_', filename)
        return filename

    def dict_publish_text(self):
        publish_text_dict = {}
        url = 'https://d33xdlntwy0kbs.cloudfront.net/cshl_custom.js'

        with open("E:\Dump\cshl_custom.js", encoding='utf-8') as f:
            page_published_text = f.read()

        soup_published_text = BeautifulSoup(page_published_text, 'html.parser')
        published_text_list = soup_published_text.select('div.pub_jnl')

        for published_text_raw in published_text_list:
            pub_text = published_text_raw.text
            if pub_text.startswith('This '):
                publish_text_dict['None'] = pub_text
            else:
                check_pub_type = re.search(r'Now (\w+) ', pub_text).groups(1)[0]
                publish_text_dict[check_pub_type] = pub_text
        return publish_text_dict

    def download_page(self, url, headers, data=None, page_name=None):
        time.sleep(random.uniform(1, 3))

        if page_name is None:
            if 'content/' in url:
                doi = url.split('content/')[1].split('v1')[0]
                page_name = f"paper_{doi.replace('/', '_')}.html"
            elif 'search/' in url:
                page_num = url.split('page=')[1].split('&')[0] if 'page=' in url else '0'
                page_name = f"search_page_{page_num}.html"
            else:
                domain = url.split('//')[1].split('/')[0]
                page_name = f"{domain.replace('.', '_')}.html"

        page_name = self.sanitize_filename(page_name)
        saved_content = self.dumper.load(url=url, file_name=page_name)

        if saved_content and self.blocked_checker.is_bad_page(saved_content):
            page = saved_content
        else:
            response = self.downloader.get(url, headers=headers)
            if not self.blocked_checker.is_blocked(response):
                page = response.text
                self.dumper.save(page, url=url, file_name=page_name)
            else:
                self.logger.error(f'请求被拦截: {url}')
                page = 'blocked'
        return page

    def get_posted_time(self, url_text):
        url_detail = f'https://www.biorxiv.org/content/{url_text}v1'
        detailed_page = self.download_page(headers=self.headers, url=url_detail)
        soup_posted = BeautifulSoup(detailed_page, 'html.parser')
        post_time_str = soup_posted.select_one('div.pane-1 div.pane-content').text.strip('\n Posted').replace(' ', '')
        return post_time_str

    def get_pub_text(self, url_text, pub_text_dict):
        """获取论文摘要"""
        url = f'https://connect.biorxiv.org/bx_pub_doi_get.php?doi={url_text}'
        page_content = self.download_page(url, headers=self.headers)
        page_content_pub = page_content.replace('\r\n\r\n\r\n\r\n', '').replace('(', '').replace(')', '').replace(
            '\n\n\n\n\n\n', '').replace('\n\n', '')
        page_content_json = json.loads(page_content_pub)
        published_type = page_content_json.get('pub')[0].get('pub_type')
        pub_doi = page_content_json.get('pub')[0].get('pub_doi')
        pub_journal = page_content_json.get('pub')[0].get('pub_journal')

        if published_type is None:
            published_type = 'None'
            pub_text = 'None'
        else:
            pub_text = pub_text_dict[published_type].replace("'+y[B].pubjournal+'", f'{pub_journal}').replace(
                '+y[B].pubdoi+"', f'{pub_doi}')
            pub_text = pub_text.replace("\'", "").replace("'", "")
        return pub_text

    def on_run(self, start_page=1, **kwargs):
        pub_text_dict = self.dict_publish_text()
        key_words = ['visium', '"10x" chromium']

        for key_word in key_words:
            safe_keyword = self.sanitize_filename(key_word)
            csv_file_name = f"articles_{safe_keyword}.csv"

            file_exists = os.path.exists(csv_file_name)

            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=[
                    'title', 'date', 'authors', 'doi',
                    'posted_time', 'publication_text', 'source_page'
                ])

                if not file_exists:
                    writer.writeheader()

                page_index = start_page - 1
                total_page = 1

                # 获取总页数
                initial_url = f'https://www.biorxiv.org/search/{parse.quote(key_word)}%20numresults%3A75%20sort%3Arelevance-rank?page=0'
                initial_page = self.download_page(url=initial_url, headers=self.headers)
                soup = BeautifulSoup(initial_page, 'html.parser')
                result_text = soup.select_one('div.highwire-search-summary').text
                total_results = int(re.search(r'([\d,]+)\sResults', result_text).group(1).replace(",", ""))
                total_page = math.ceil(total_results / 75)

                self.logger.debug(
                    f"关键词 '{key_word}' 总结果数: {total_results} (共{total_page}页), 从第{start_page}页开始爬取")

                while page_index < int(total_page) - 1:
                    page_index += 1

                    if page_index % 10 == 0:
                        time.sleep(random.uniform(5, 10))

                    url = f'https://www.biorxiv.org/search/{parse.quote(key_word)}%20numresults%3A75%20sort%3Arelevance-rank?page={page_index}'
                    page = self.download_page(url=url, headers=self.headers)
                    soup = BeautifulSoup(page, 'html.parser')
                    articles = soup.select(".highwire-search-results-list > li")

                    for article in articles:
                        try:
                            title = article.select_one(".highwire-cite-linked-title > .highwire-cite-title").text
                            date_id = article.select_one(".highwire-cite-metadata-pages").text
                            authors = article.select_one(".highwire-cite-authors").text
                            doi_label = article.select_one(".doi_label").next_sibling
                            url_text = doi_label.replace(' ', '')

                            # 获取详细信息
                            posted_time = self.get_posted_time(url_text)
                            pub_text = self.get_pub_text(url_text, pub_text_dict)

                            # 记录来源页码
                            source_page = f"page_{page_index}"

                            # 写入CSV
                            writer.writerow({
                                'title': title,
                                'date': date_id,
                                'authors': authors,
                                'doi': url_text,
                                'posted_time': posted_time,
                                'publication_text': pub_text,
                                'source_page': source_page
                            })

                            self.logger.debug(f"已保存: {title[:50]}...")

                        except Exception as e:
                            self.logger.error(f"处理论文失败: {str(e)}")
                            continue

                    self.logger.info(f"已完成 {key_word} 第{page_index}/{total_page}页的保存")


if __name__ == '__main__':
    a = BioScraper()
    a.on_run()
