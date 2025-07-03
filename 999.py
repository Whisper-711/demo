import requests
from bs4 import BeautifulSoup
import json
import datetime
import re
import math
import pandas as pd
import logging
import os
from urllib import parse
from pathlib import Path
import time
from typing import Dict, List, Optional, Tuple, Any


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("biorxiv_scraper.log")
    ]
)
logger = logging.getLogger("BiorxivScraper")

class Paper:
    def __init__(self):
        self.search_keyword = None
        self.title = None
        self.authors = None
        self.doi_date = None
        self.doi_ID = None
        self.detail_url = None
        self.posted_time = None
        self.posted_time_raw = None
        self.publish_text = None
        self.source_url = None
        self.run_id = None
        self.run_date = None
        self.insert_update_time = None

    def to_dict(self):
        """将论文对象转换为字典形式"""
        return {
            'search_keyword': self.search_keyword,
            'title': self.title,
            'authors': self.authors,
            'doi_date': self.doi_date,
            'doi_ID': self.doi_ID,
            'detail_url': self.detail_url,
            'posted_time': self.posted_time,
            'posted_time_raw': self.posted_time_raw,
            'publish_text': self.publish_text,
            'source_url': self.source_url,
            'run_id': self.run_id,
            'run_date': self.run_date,
            'insert_update_time': self.insert_update_time
        }

class HTTPSession:
    def __init__(self,timeout: int = 30):
        self.session = requests.Session()
        self.timeout = timeout
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Connection': 'keep-alive'
        }

    def set_header(self,key:str,value:str):
        self.headers[key] = value

    def get(self,url:str, validate_str_list: List[str] = None):
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                response = self.session.get(url,headers = self.headers,timeout=self.timeout)
                response.raise_for_status()
                content = response.text

                if validate_str_list:
                    if all(validate_str in content for validate_str in validate_str_list):
                        return content
                    else:
                        missing = [s for s in validate_str_list if s not in content]
                        logger.warning(f"内容验证失败，缺少字符串: {missing}")
                else:
                    return content
            except (requests.RequestException, Exception) as e:
                logger.error(f"请求失败：{url},错误：{str(e)}")

class BiorxivScraper:
    def __init__(self, output_dir: str = "data"):
        self.output_dir = output_dir
        self.run_date = datetime.datetime.now()
        self.papers = []

        # 确保输出目录存在
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # 初始化HTTP会话
        self.detail_session = HTTPSession(timeout=60)
        self.detail_session.set_header('Host', 'www.biorxiv.org')
        self.detail_session.set_header('Sec-Fetch-Site', 'none')
        self.detail_session.set_header('Sec-Fetch-Mode', 'navigate')

        self.connect_session = HTTPSession(timeout=60)
        self.connect_session.set_header('Host', 'connect.biorxiv.org')
        self.connect_session.set_header('Referer', 'https://www.biorxiv.org/')

        self.js_session = HTTPSession(timeout=60)
        self.js_session.set_header('Host', 'd33xdlntwy0kbs.cloudfront.net')
        self.js_session.set_header('Referer', 'https://www.biorxiv.org/')
        self.js_session.set_header('Sec-Fetch-Site', 'cross-site')
        self.js_session.set_header('Sec-Fetch-Mode', 'no-cors')

    def get_publish_text_dict(self):
        """获取发布文本类型的字典"""
        publish_text_dict = {}
        text_url = 'https://d33xdlntwy0kbs.cloudfront.net/cshl_custom.js'

        page_content = self.js_session.get(text_url, validate_str_list=['pub_jnl'])
        soup = BeautifulSoup(page_content, 'html.parser')
        published_text_list = soup.select('div.pub_jnl')

        for text_element in published_text_list:
            pub_text = text_element.text
            if pub_text.startswith('This '):
                publish_text_dict['None'] = pub_text
            else:
                match = re.search(r'Now (\w+) ', pub_text)
                if match:
                    pub_type = match.group(1)
                    publish_text_dict[pub_type] = pub_text

        return publish_text_dict

    def get_pub_text(self, url_text: str, pub_text_dict: Dict[str, str]) -> str:
        """获取论文的发布信息文本"""
        url_paper_detail = f'https://connect.biorxiv.org/bx_pub_doi_get.php?doi={url_text}'

        paper_content = self.connect_session.get(url_paper_detail, validate_str_list=['pub'])
        clean_content = (paper_content
                             .replace('\r\n\r\n\r\n\r\n', '')
                             .replace('(', '')
                             .replace(')', '')
                             .replace('\n\n\n\n\n\n', '')
                             .replace('\n\n', ''))

        json_data = json.loads(clean_content)
        if not json_data.get('pub') or len(json_data['pub']) == 0:
            return 'None'

        pub_info = json_data['pub'][0]
        published_type = pub_info.get('pub_type')
        pub_doi = pub_info.get('pub_doi', '')
        pub_journal = pub_info.get('pub_journal', '')

        if not published_type:
            return 'None'

        template = pub_text_dict.get(published_type, pub_text_dict.get('None', 'None'))
        pub_text = template.replace("'+y[B].pubjournal+'", pub_journal).replace(
                '+y[B].pubdoi+"', pub_doi)

        return pub_text.replace("\'", "").replace("'", "")

    def scrape_search_results(self, keywords: List[str], max_pages: int = None) -> List[Paper]:
        """抓取搜索结果中的论文信息"""
        pub_text_dict = self.get_publish_text_dict()
        run_timestamp = self.run_date.strftime("%Y-%m-%d %H:%M:%S")

        for keyword in keywords:
            logger.info(f"开始抓取关键词 '{keyword}' 的数据")
            page_index = 0
            total_page = 1

            while page_index < total_page:
                # 构建搜索URL
                url = f'https://www.biorxiv.org/search/{parse.quote(keyword)}%20numresults%3A75%20sort%3Arelevance-rank?page={page_index}'
                logger.info(f"处理页面 {page_index + 1}")

                try:
                    page_content = self.detail_session.get(url, validate_str_list=['highwire-search-results-list'])
                    soup = BeautifulSoup(page_content, 'html.parser')

                    # 获取结果总数和总页数
                    summary_text = soup.select_one('div.highwire-search-summary').text
                    total_results_match = re.search(r'([\d,]+)\sResults', summary_text)

                    if total_results_match:
                        total_results = int(total_results_match.group(1).replace(",", ""))
                        total_page = math.ceil(total_results / 75)

                        if max_pages and max_pages < total_page:
                            total_page = max_pages

                        logger.info(f"找到 {total_results} 个结果，共 {total_page} 页")

                    # 处理每篇文章
                    articles = soup.select(".highwire-search-results-list > li")
                    logger.info(f"当前页面包含 {len(articles)} 篇文章")

                    for article_block in articles:
                        try:
                            paper = Paper()

                            # 提取基本信息
                            title_element = article_block.select_one(
                                ".highwire-cite-linked-title > .highwire-cite-title")
                            if title_element:
                                paper.title = title_element.text

                            date_id_element = article_block.select_one(".highwire-cite-metadata-pages")
                            if date_id_element:
                                date_id = date_id_element.text

                            authors_element = article_block.select_one(".highwire-cite-authors")
                            if authors_element:
                                paper.authors = authors_element.text

                            doi_label_element = article_block.select_one(".doi_label")
                            if doi_label_element and doi_label_element.next_sibling:
                                doi_text = doi_label_element.next_sibling
                                url_text = doi_text.replace('https://doi.org/', '').replace(' ', '')

                                # 设置论文基本属性
                                paper.source_url = url
                                paper.detail_url = f'https://www.biorxiv.org/content/{url_text}v1'
                                paper.search_keyword = keyword

                                # 处理日期和ID
                                date_id = date_id.split(";")[0]
                                if "." in date_id:
                                    paper.doi_ID = date_id.split(".")[-1]
                                    date_raw = date_id.rsplit('.', 1)[0]
                                    paper.doi_date = str(datetime.datetime.strptime(date_raw, '%Y.%m.%d').date())
                                else:
                                    paper.doi_ID = date_id
                                    paper.doi_date = None

                                # 获取发布时间
                                posted_time_raw = self.get_posted_time(url_text)
                                if 'Youarenotauthorizedtoaccessthispage' in posted_time_raw:
                                    paper.posted_time_raw = None
                                    paper.posted_time = None
                                else:
                                    paper.posted_time_raw = posted_time_raw
                                    time_raw = (posted_time_raw
                                                .replace(' ', '')
                                                .replace('\xa0', '')
                                                .replace('NBSP', '')
                                                .strip('.'))
                                    try:
                                        time_format = datetime.datetime.strptime(time_raw, '%B%d,%Y')
                                        paper.posted_time = datetime.datetime.strftime(time_format, '%Y-%m-%d %H:%M:%S')
                                    except ValueError:
                                        logger.warning(f"无法解析日期: {time_raw}")
                                        paper.posted_time = None

                                # 获取发布文本
                                paper.publish_text = self.get_pub_text(url_text, pub_text_dict)

                                paper.run_id = run_timestamp
                                paper.run_date = run_timestamp
                                paper.insert_update_time = run_timestamp

                                self.papers.append(paper)
                                logger.info(f"成功处理论文: {paper.title}")

                        except Exception as e:
                            logger.error(f"处理文章时出错: {str(e)}")

                    page_index += 1
                    time.sleep(2)

                except Exception as e:
                    logger.error(f"处理搜索页面时出错: {str(e)}")
                    break

        return self.papers

    def save_to_csv(self, filename_template: str = "biorxiv_{}.csv") -> str:
        """将抓取的论文保存到CSV文件"""
        if not self.papers:
            logger.warning("没有论文可保存")
            return None

        filename = filename_template.format(self.run_date.strftime("%Y-%m-%d"))
        filepath = os.path.join(self.output_dir, filename)

        # 转换论文列表为DataFrame
        papers_dict = [paper.to_dict() for paper in self.papers]
        df = pd.DataFrame(papers_dict)

        # 保存到CSV
        df.to_csv(filepath, index=False)
        logger.info(f"数据已保存到 {filepath}, 共 {len(self.papers)} 篇论文")

        return filepath

def main():
    """主函数"""
    # 创建爬虫实例
    scraper = BiorxivScraper(output_dir="data")

    # 设置搜索关键词
    keywords = ['visium', '"10x" chromium']

    try:
        # 抓取数据
        logger.info("开始抓取bioRxiv数据")
        papers = scraper.scrape_search_results(keywords, max_pages=None)

        # 保存结果
        if papers:
            csv_path = scraper.save_to_csv()
            logger.info(f"爬取完成，共获取 {len(papers)} 篇论文，数据已保存到 {csv_path}")
        else:
            logger.warning("未找到任何论文")

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")


if __name__ == "__main__":
    main()
