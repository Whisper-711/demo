import logging
import math
import os
import re
import time
import random
from pathlib import Path
from urllib import parse
import csv
from bs4 import BeautifulSoup
from degree72.implementations.blocked_checkers.request_blocked_checker import RequestBlockedChecker
from degree72.implementations.daos.csv_dao import CsvDao
from degree72.implementations.downloaders.request_downloader import RequestDownloader
from degree72.implementations.dump_managers.local_dump_manager import LocalDumpManager
from degree72.utils.http.header_utils import parse_fiddler_headers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("biorxiv_scraper.log")
    ]
)
logger = logging.getLogger("BiorxivScraper")


class BioScraper:
    def __init__(self):

        self.logger = logging.getLogger("BiorxivScraper")

        self.blocked_checker = RequestBlockedChecker()

        self.proxy = {
            'http': 'http://127.0.0.1:7897',
            'https': 'http://127.0.0.1:7897'
        }

        # 初始化下载器时传入代理
        self.downloader = RequestDownloader(
            blocked_checker=self.blocked_checker,
            proxies=self.proxy  # 添加代理配置
        )

        storage_dir = Path(__file__).parent / "biorxiv_data"
        self.dumper = LocalDumpManager(
            project="biorxiv_scraper",
            base_dir=str(storage_dir)
        )

        self.dao = CsvDao()
        
        # Multiple realistic User-Agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0'
        ]
        
        # More sophisticated base headers
        self.base_headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="126", "Chromium";v="126"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Cookie':'_ga=GA1.1.57067.1751437669; SSESS1dd6867f1a1b90340f573dcdef3076bc=RYWR8fgx14aOl6stwKKM1EQtAUAAR-HWXGndXQ3Lw9U; cookie-agreed=2; __cf_bm=mAMkB1tzXAM.zJSwOZIL9OZd_4aNOLaAUr2le2LXTOY-1751591942-1.0.1.1-usIE2396WjAQZ_kmhjJ7_kScjrxftn5g6mmvlGPQpd4Ygp1gGCPVzXESzeavz0pUIt3_ZEG7v_SWiKTxiCgPUXCuW2LMCD5qtHjVp2z1eZk; has_js=1; _ga_RZD586MC3Q=GS2.1.s1751591942$o3$g1$t1751591948$j54$l0$h0'
        }

        self.detail_headers = self.base_headers.copy()
        self.detail_headers.update({
            'Host': 'www.biorxiv.org',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Referer': 'https://www.biorxiv.org/'
        })

        self.connect_headers = self.base_headers.copy()
        self.connect_headers.update({
            'Host': 'connect.biorxiv.org',
            'Referer': 'https://www.biorxiv.org/',
            'Origin': 'https://www.biorxiv.org',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty'
        })

        self.js_headers = self.base_headers.copy()
        self.js_headers.update({
            'Host': 'd33xdlntwy0kbs.cloudfront.net',
            'Referer': 'https://www.biorxiv.org/',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Dest': 'script',
            'Accept': '*/*'
        })


    def dict_publish_text(self):
        publish_text_dict = {}
        # 获取种类信息
        url = 'https://d33xdlntwy0kbs.cloudfront.net/cshl_custom.js'
        response = self.downloader.get(url, headers=self.js_headers)

        if hasattr(response, 'text'):
            page_published_text = response.text
        else:
            page_published_text = str(response)

        # 解析内容
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

    def _rotate_headers(self, headers):
        """Rotate user-agent and update headers to appear more human-like"""
        new_headers = headers.copy()
        new_headers['User-Agent'] = random.choice(self.user_agents)
        
        # Add slightly random accept headers
        accept_variations = [
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
        ]
        new_headers['Accept'] = random.choice(accept_variations)
        return new_headers

    def download_page(self, url, headers, data=None, page_name=None):
        # Add random delay between 2-5 seconds to mimic human behavior
        time.sleep(random.uniform(2, 5))
        
        # Rotate headers for each request
        rotated_headers = self._rotate_headers(headers)
        
        saved_content = self.dumper.load(url=url, file_name=page_name)
        if saved_content and self.blocked_checker.is_bad_page(saved_content):
            page = saved_content
        else:
            response = self.downloader.get(url, headers=rotated_headers)
            if not self.blocked_checker.is_blocked(response):
                page = response.text
                self.dumper.save(page, url=url, file_name=page_name)
            else:
                self.logger.error(f'Request blocked for URL: {url}')
                page = 'blocked'
        return page

    def get_posted_time(self, url_text: str) -> str:
        """获取论文发布时间"""
        url = f'https://www.biorxiv.org/content/{url_text}v1'
        page_content = self.download_page(url, headers=self.detail_headers)
        if page_content:
            soup = BeautifulSoup(page_content, 'html.parser')

            # 尝试通过更精确的选择器获取发布日期
            date_element = soup.select_one('.pane-1 .pane-content')
            if date_element:
                # 使用正则表达式提取日期格式
                date_text = date_element.text.strip()
                date_match = re.search(
                    r'Posted\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})',
                    date_text)
                if date_match:
                    month = date_match.group(1)
                    day = date_match.group(2)
                    year = date_match.group(3)
                    return f"{month} {day}, {year}"

    def get_pub_text(self, url_text: str, pub_text_dict: dict) -> str:
        """获取论文摘要"""
        url = f'https://connect.biorxiv.org/bx_pub_doi_get.php?doi={url_text}'
        page_content = self.download_page(url, headers=self.connect_headers)
        if page_content:
            soup = BeautifulSoup(page_content, 'html.parser')
            pub_text = soup.select_one('div.pub_jnl').text
            if pub_text.startswith('This '):
                pub_text = pub_text_dict['None']
            else:
                check_pub_type = re.search(r'Now (\w+) ', pub_text).groups(1)[0]
                pub_text = pub_text_dict[check_pub_type]
            return pub_text

    def on_run(self,**kwargs):
        pub_text_dict = self.dict_publish_text()
        key_words = ['visium', '"10x" chromium']
        for key_word in key_words:
            page_index = -1
            total_page = 1
            while page_index < int(total_page) - 1:
                page_index += 1
                url = 'https://www.biorxiv.org/search/{}%20numresults%3A75%20sort%3Arelevance-rank?page={}'.format(
                    parse.quote(key_word), page_index)

                # Set referer to simulate coming from homepage for first page, or previous page for subsequent pages
                temp_headers = self.detail_headers.copy()
                if page_index == 0:
                    temp_headers['Referer'] = 'https://www.biorxiv.org/'
                else:
                    temp_headers['Referer'] = 'https://www.biorxiv.org/search/{}%20numresults%3A75%20sort%3Arelevance-rank?page={}'.format(
                        parse.quote(key_word), page_index-1)

                page = self.download_page(url, headers=temp_headers)
                soup = BeautifulSoup(page, 'html.parser')
                
                # Check if we got a valid page
                result_summary = soup.select_one('div.highwire-search-summary')
                if not result_summary:
                    self.logger.error(f"Failed to get valid results page for keyword '{key_word}' page {page_index}")
                    continue
                    
                result_text = result_summary.text
                total_results = int(re.search(r'([\d,]+)\sResults', result_text).group(1).replace(",", ""))
                total_page = math.ceil(total_results / 75)
                articles = soup.select(".highwire-search-results-list > li")
                self.logger.info(f"processing {page_index * 75}--{total_results}")
                articles_storage = []

                for article in articles:
                    title = article.select_one(".highwire-cite-linked-title > .highwire-cite-title").text
                    date_id = article.select_one(".highwire-cite-metadata-pages").text
                    authors = article.select_one(".highwire-cite-authors").text
                    doi_label = article.select_one(".doi_label").next_sibling
                    url_text = doi_label.replace(' ', '')

                    # 提取更多详细信息
                    posted_time = self.get_posted_time(url_text)
                    pub_text = self.get_pub_text(url_text, pub_text_dict)

                    # 存储文章信息
                    article_info = {
                        'title': title,
                        'date': date_id,
                        'authors': authors,
                        'doi': url_text,
                        'posted_time': posted_time,
                        'publication_text': pub_text
                    }
                    articles_storage.append(article_info)
                
                # After processing each page, add a longer delay to avoid detection
                time.sleep(random.uniform(5, 10))
                
                csv_file_name = f"articles_{key_word}_{page_index}.csv"
                with open(csv_file_name, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['title', 'date', 'authors', 'doi', 'posted_time', 'publication_text']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for article_info in articles_storage:
                        writer.writerow(article_info)

                self.logger.info(f"Data for {key_word} page {page_index} has been written to {csv_file_name}")
                
                # Add a longer random delay between pages
                self.logger.info(f"Taking a break before proceeding to next page...")
                time.sleep(random.uniform(10, 20))

if __name__ == '__main__':
    a = BioScraper()
    a.on_run() 











    def on_run(self, **kwargs):
        pub_text_dict = self.dict_publish_text()
        key_words = ['visium', '"10x" chromium']
        for key_word in key_words:
            page_index = -1
            total_page = 1
            while page_index < int(total_page) - 1:
                page_index += 1
                # 'https://www.biorxiv.org/search/chromium%20numresults%3A75%20sort%3Arelevance-rank?page=1
                url = 'https://www.biorxiv.org/search/{}%20numresults%3A75%20sort%3Arelevance-rank?page={}'.format(
                    parse.quote(key_word), page_index)

                page = self.clever_download_page(url, self.detail_manager, validate_str_list=['highwire-search-results-list'])
                soup = BeautifulSoup(page, 'html.parser')
                result_text = soup.select_one('div.highwire-search-summary').text
                total_results = int(re.search(r'([\d,]+)\sResults', result_text).group(1).replace(",", ""))
                total_page = math.ceil(total_results / 75)
                # total_page = 10 # only for debug
                articles = soup.select(".highwire-search-results-list > li")
                self.log.info("processing", "{}--{}".format(page_index * 75, total_results))
                for article_block in articles:
                    try:
                        en = Entity()
                        title = article_block.select_one(".highwire-cite-linked-title > .highwire-cite-title").text
                        date_id = article_block.select_one(".highwire-cite-metadata-pages").text
                        authors = article_block.select_one(".highwire-cite-authors").text
                        doi_label = article_block.select_one(".doi_label").next_sibling
                        url_text = doi_label.replace('https://doi.org/', '').replace(' ', '')

                        en.source_url = url
                        en.detail_url = 'https://www.biorxiv.org/content/{}v1'.format(url_text)
                        en.search_keyword = key_word
                        en.title = title
                        en.authors = authors
                        date_id = date_id.split(";")[0]
                        if "." in date_id:
                            en.doi_ID = date_id.split(".")[-1]
                            date_raw = date_id.rsplit('.', 1)[0]
                            en.doi_date = str(datetime.datetime.strptime(date_raw, '%Y.%m.%d').date())
                        else:
                            en.doi_ID = date_id
                            en.doi_date = None
                        #   August 06, 2021. 是posted_time_raw的举例
                        posted_time_raw = self.get_posted_time(url_text)
                        check_str = 'Youarenotauthorizedtoaccessthispage'
                        if check_str in posted_time_raw:
                            # continue
                            en.posted_time_raw = None
                            en.posted_time = None
                        else:
                            en.posted_time_raw = posted_time_raw
                            time_raw = posted_time_raw.replace(' ', '').replace('\xa0', '').replace('NBSP', '').strip('.')#.strip(' ')
                            time_format = datetime.datetime.strptime(time_raw, '%B%d,%Y')
                            en.posted_time = datetime.datetime.strftime(time_format, '%Y-%m-%d %H:%M:%S')
                        en.publish_text = self.get_pub_text(url_text, pub_text_dict)
                        self._dao.save(en)

                    except Exception as e:

                        self.log.error("failed to process article block", "{}--{}".format(str(e), str(article_block)))
