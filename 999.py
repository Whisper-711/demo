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
import argparse


# 创建日志记录器
logger = logging.getLogger("BiorxivScraper")

def setup_logging(debug_mode=False):
    """设置日志级别和格式"""
    # 清除之前的处理程序
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 设置日志级别
    if debug_mode:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    # 创建控制台处理程序
    console_handler = logging.StreamHandler()
    
    # 创建文件处理程序
    file_handler = logging.FileHandler("biorxiv_scraper.log")
    
    # 设置日志格式
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # 添加处理程序到记录器
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # 设置过滤器，在非调试模式下过滤掉DEBUG级别的消息和论文处理成功的消息
    if not debug_mode:
        class CustomFilter(logging.Filter):
            def filter(self, record):
                # 只允许ERROR级别的消息和特定的INFO级别消息通过
                # 过滤掉包含"成功处理论文"、"进度更新"和"当前页面包含"的INFO消息
                if record.levelno == logging.INFO:
                    msg = record.getMessage()
                    if any(text in msg for text in ["成功处理论文", "进度更新", "当前页面包含", "处理页面"]):
                        return False
                return record.levelno == logging.ERROR or record.levelno == logging.INFO
        
        console_handler.addFilter(CustomFilter())

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

class BiorxivScraper:
    def __init__(self, output_dir: str = "data", proxy: str = "127.0.0.1:7890", debug_mode: bool = False):
        self.output_dir = output_dir
        self.run_date = datetime.datetime.now()
        self.papers = []
        self.proxy = proxy
        self.timeout = 60
        self.debug_mode = debug_mode
        
        # Cloudflare绕过的Cookie
        self.cf_cookies = "SSESS1dd6867f1a1b90340f573dcdef3076bc=oVHn_uCw4M8eq8LQPRzflmQrj8BBvkV4jQjFy7au5bc; _ga=GA1.1.1571510242.1751423136; cookie-agreed=2; has_js=1; dsq__u=8lptgnv265udnd; dsq__s=8lptgnv265udnd; _li_dcdm_c=.biorxiv.org; _lc2_fpi=28e3293678dc--01jz72fatjsknsw5j41wfvk17m; _lc2_fpi_js=28e3293678dc--01jz72fatjsknsw5j41wfvk17m; _li_ss=CgA; cf_clearance=pkCxeksGinNQPuh8M1DT1_8zROC8dq1WMswGXa9ijGg-1751512637-1.2.1.1-vKJWa4axS8Msn8GOi_OB9QfXanKLtQGmKl524d84v4ec3NJsUePWSO2dM.oZh5Fx6y0Db5X5H9szYp2iEpLCLjEj5WbZsgEHlC6Qy7pE.fmPnKKH8qm7ZaMZcCTBeCdnOgndouU74meRMSlYIV4iB3cDKNF6Rz8JGmDxu6exiOndFkvNaVKJq6E_fUYpyFQ1Er1s9cT.s4orDqgAc_U__qZEK07tjcWzslh4EQTY1TU; _ga_RZD586MC3Q=GS2.1.s1751520265$o6$g0$t1751520265$j60$l0$h0; __cf_bm=AsC2SOgZzAZifnh.btYBQGpyYUV6cTeNw71A.NkGJpI-1751520281-1.0.1.1-im.w5z.U9kbb6VX5r.uHOkXr_75GIf18A9iRqp6GKj9oAdqcWNcBD99Llhcf0ZP24fWeIuWEzeVtbvuND7cNbzzviBLs84sXoVakqC.y_WE"
        
        # 基础请求头
        self.base_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Connection': 'keep-alive',
            'Cookie': self.cf_cookies
        }
        
        # 代理设置
        self.proxies = None
        if proxy:
            self.proxies = {
                'http': f'http://{proxy}',
                'https': f'http://{proxy}'
            }

        # 确保输出目录存在
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # 初始化不同域名的请求头
        self.detail_headers = self.base_headers.copy()
        self.detail_headers.update({
            'Host': 'www.biorxiv.org',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate'
        })
        
        self.connect_headers = self.base_headers.copy()
        self.connect_headers.update({
            'Host': 'connect.biorxiv.org',
            'Referer': 'https://www.biorxiv.org/'
        })
        
        self.js_headers = self.base_headers.copy()
        self.js_headers.update({
            'Host': 'd33xdlntwy0kbs.cloudfront.net',
            'Referer': 'https://www.biorxiv.org/',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-Mode': 'no-cors'
        })

    def make_request(self, url: str, headers: Dict[str, str], validate_str_list: List[str] = None) -> str:
        """发送HTTP请求并获取内容"""
        max_retries = 5  # 增加重试次数
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                logger.debug(f"尝试请求URL: {url} (第{retry_count + 1}次尝试)")
                response = requests.get(
                    url, 
                    headers=headers, 
                    proxies=self.proxies, 
                    timeout=self.timeout
                )
                response.raise_for_status()
                content = response.text
                
                # 检查是否遇到Cloudflare验证
                if "Just a moment..." in content or "Checking if the site connection is secure" in content:
                    logger.warning(f"遇到Cloudflare验证，尝试使用Cookie绕过")
                    retry_count += 1
                    time.sleep(5)
                    continue
                
                if validate_str_list:
                    missing = [s for s in validate_str_list if s not in content]
                    if not missing:
                        logger.debug(f"内容验证成功")
                        return content
                    else:
                        logger.warning(f"内容验证失败，缺少字符串: {missing}")
                        # 保存失败的响应内容以便调试
                        if self.debug_mode:
                            error_file = f"error_response_{int(time.time())}.html"
                            with open(error_file, "w", encoding="utf-8") as f:
                                f.write(content)
                            logger.debug(f"已将错误响应保存到 {error_file}")
                        
                        # 增加重试间隔时间
                        retry_count += 1
                        wait_time = retry_count * 2  # 指数退避
                        logger.debug(f"等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                else:
                    return content
            except requests.RequestException as e:
                logger.error(f"请求失败：{url},错误：{str(e)}")
                retry_count += 1
                wait_time = retry_count * 2  # 指数退避
                logger.debug(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            except Exception as e:
                logger.error(f"未知错误：{url},错误：{str(e)}")
                retry_count += 1
                wait_time = retry_count * 2  # 指数退避
                logger.debug(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
        
        logger.error(f"请求失败，已达到最大重试次数: {url}")
        return ""

    def update_cookies(self, new_cookies: str):
        """更新Cookie，用于在运行时更新Cloudflare验证信息"""
        self.cf_cookies = new_cookies
        
        # 更新所有请求头中的Cookie
        self.base_headers['Cookie'] = new_cookies
        self.detail_headers['Cookie'] = new_cookies
        self.connect_headers['Cookie'] = new_cookies
        self.js_headers['Cookie'] = new_cookies
        
        logger.info("已更新Cookie")

    def get_publish_text_dict(self):
        """获取发布文本类型的字典"""
        publish_text_dict = {}
        text_url = 'https://d33xdlntwy0kbs.cloudfront.net/cshl_custom.js'

        page_content = self.make_request(text_url, self.js_headers, validate_str_list=['pub_jnl'])
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

        paper_content = self.make_request(url_paper_detail, self.connect_headers, validate_str_list=['pub'])
        if not paper_content:
            return 'None'
            
        try:
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
        except Exception as e:
            logger.error(f"处理发布文本时出错: {str(e)}")
            return 'None'
        
    def get_posted_time(self, url_text: str) -> str:
        """获取论文发布时间"""
        url = f'https://www.biorxiv.org/content/{url_text}v1'
        page_content = self.make_request(url, self.detail_headers)
        if page_content:
            soup = BeautifulSoup(page_content, 'html.parser')
            
            # 尝试通过更精确的选择器获取发布日期
            date_element = soup.select_one('.pane-1 .pane-content')
            if date_element:
                # 使用正则表达式提取日期格式
                date_text = date_element.text.strip()
                date_match = re.search(r'Posted\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})', date_text)
                if date_match:
                    month = date_match.group(1)
                    day = date_match.group(2)
                    year = date_match.group(3)
                    return f"{month} {day}, {year}"
            
            # 如果上面的方法失败，尝试其他选择器
            posted_element = soup.select_one('.pane-content')
            if posted_element:
                date_text = posted_element.text.strip()
                # 过滤掉不包含日期的文本
                if "Search for this keyword" in date_text:
                    return ""
                return date_text
        return ""

    def scrape_search_results(self, keywords: List[str], max_pages: int = None, max_papers: int = None) -> List[Paper]:
        """抓取搜索结果中的论文信息
        
        Args:
            keywords: 搜索关键词列表
            max_pages: 最大页数限制
            max_papers: 最大论文数量限制，用于测试
        """
        pub_text_dict = self.get_publish_text_dict()
        run_timestamp = self.run_date.strftime("%Y-%m-%d %H:%M:%S")
        total_papers_processed = 0
        papers_per_keyword = {}  # 记录每个关键词抓取的论文数量

        for keyword in keywords:
            logger.debug(f"开始抓取关键词 '{keyword}' 的数据")
            page_index = 0
            total_page = 1
            papers_per_keyword[keyword] = 0
            keyword_papers_count = 0

            while page_index < total_page:
                # 检查是否达到最大论文数量限制
                if max_papers and total_papers_processed >= max_papers:
                    logger.debug(f"已达到最大论文数量限制 ({max_papers})，停止抓取")
                    return self.papers
                
                # 构建搜索URL
                url = f'https://www.biorxiv.org/search/{parse.quote(keyword)}%20numresults%3A75%20sort%3Arelevance-rank?page={page_index}'
                logger.debug(f"处理页面 {page_index + 1}")

                try:
                    # 增加页面加载等待时间，避免被网站限制
                    time.sleep(3)  
                    
                    page_content = self.make_request(url, self.detail_headers, validate_str_list=['highwire-search-results-list'])
                    if not page_content:
                        logger.error(f"无法获取页面内容，跳过页面 {page_index + 1}")
                        page_index += 1
                        continue
                        
                    soup = BeautifulSoup(page_content, 'html.parser')

                    # 获取结果总数和总页数
                    summary_text = soup.select_one('div.highwire-search-summary')
                    if not summary_text:
                        logger.error(f"无法找到搜索结果摘要，跳过页面 {page_index + 1}")
                        page_index += 1
                        continue
                        
                    summary_text = summary_text.text
                    total_results_match = re.search(r'([\d,]+)\sResults', summary_text)

                    if total_results_match:
                        total_results = int(total_results_match.group(1).replace(",", ""))
                        total_page = math.ceil(total_results / 75)

                        if max_pages and max_pages < total_page:
                            total_page = max_pages

                        logger.debug(f"找到 {total_results} 个结果，共 {total_page} 页")

                    # 处理每篇文章
                    articles = soup.select(".highwire-search-results-list > li")
                    logger.debug(f"当前页面包含 {len(articles)} 篇文章")

                    for article_block in articles:
                        # 检查是否达到最大论文数量限制
                        if max_papers and total_papers_processed >= max_papers:
                            logger.debug(f"已达到最大论文数量限制 ({max_papers})，停止抓取")
                            return self.papers
                            
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
                                if not posted_time_raw or 'Youarenotauthorizedtoaccessthispage' in posted_time_raw:
                                    paper.posted_time_raw = None
                                    paper.posted_time = None
                                else:
                                    paper.posted_time_raw = posted_time_raw
                                    time_raw = (posted_time_raw
                                                .replace('\xa0', ' ')
                                                .replace('NBSP', ' ')
                                                .strip('.'))
                                    try:
                                        # 尝试多种日期格式
                                        date_formats = [
                                            '%B %d, %Y',  # January 1, 2020
                                            '%B%d,%Y',    # January1,2020
                                        ]
                                        
                                        parsed_date = None
                                        for date_format in date_formats:
                                            try:
                                                parsed_date = datetime.datetime.strptime(time_raw, date_format)
                                                break
                                            except ValueError:
                                                continue
                                        
                                        if parsed_date:
                                            paper.posted_time = datetime.datetime.strftime(parsed_date, '%Y-%m-%d %H:%M:%S')
                                        else:
                                            logger.warning(f"无法解析日期: {time_raw}")
                                            paper.posted_time = None
                                    except Exception as e:
                                        logger.warning(f"日期解析错误: {time_raw}, 错误: {str(e)}")
                                        paper.posted_time = None

                                # 获取发布文本
                                paper.publish_text = self.get_pub_text(url_text, pub_text_dict)

                                paper.run_id = run_timestamp
                                paper.run_date = run_timestamp
                                paper.insert_update_time = run_timestamp

                                self.papers.append(paper)
                                total_papers_processed += 1
                                keyword_papers_count += 1
                                papers_per_keyword[keyword] = keyword_papers_count
                                
                                # 在调试模式下打印每篇论文的处理信息
                                logger.debug(f"成功处理论文: {paper.title} ({total_papers_processed}/{max_papers if max_papers else '无限制'}) [关键词'{keyword}': {keyword_papers_count}篇]")

                        except Exception as e:
                            logger.error(f"处理文章时出错: {str(e)}")

                    page_index += 1
                    time.sleep(3)

                except Exception as e:
                    logger.error(f"处理搜索页面时出错: {str(e)}")
                    # 尝试继续下一页
                    page_index += 1
                    time.sleep(5)

        for keyword, count in papers_per_keyword.items():
            logger.debug(f"关键词 '{keyword}' 共抓取了 {count} 篇论文")
            
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

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='BioRxiv论文爬虫')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    parser.add_argument('--max-papers', type=int, default=None, help='最大论文数量限制')
    parser.add_argument('--max-pages', type=int, default=None, help='最大页数限制')
    parser.add_argument('--output-dir', type=str, default="data", help='输出目录')
    parser.add_argument('--keywords', type=str, nargs='+', default=['visium', '"10x" chromium'], help='搜索关键词列表')
    parser.add_argument('--update-cookie', type=str, help='更新Cloudflare验证Cookie')
    
    return parser.parse_args()

def main():
    """主函数"""
    # 解析命令行参数
    args = parse_arguments()
    
    # 设置日志级别
    setup_logging(debug_mode=args.debug)
    
    # 创建爬虫实例，设置固定代理
    proxy = "127.0.0.1:7890"  # 固定代理地址
    scraper = BiorxivScraper(output_dir=args.output_dir, proxy=proxy, debug_mode=args.debug)
    
    # 如果提供了新的Cookie，则更新
    if args.update_cookie:
        scraper.update_cookies(args.update_cookie)

    # 设置搜索关键词
    keywords = args.keywords
    
    # 设置测试模式参数
    max_papers = None    # 设置为None可以抓取所有论文
    max_pages = None   # 设置为None可以抓取所有页面

    try:
        # 抓取数据
        logger.info(f"开始抓取bioRxiv数据，使用代理: {proxy}")
        logger.info(f"运行模式: {'调试模式' if args.debug else '正常模式'}")
        logger.info(f"搜索关键词: {keywords}")
        logger.info(f"限制: 最多抓取 {max_papers if max_papers else '无限制'} 篇论文，最多 {max_pages if max_pages else '无限制'} 页")
        
        # 记录开始时间
        start_time = time.time()
        
        papers = scraper.scrape_search_results(keywords, max_pages=max_pages, max_papers=max_papers)

        # 记录结束时间并计算耗时
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if papers:
            # 保存为CSV
            csv_path = scraper.save_to_csv()
            
            logger.info(f"爬取完成，共获取 {len(papers)} 篇论文，耗时: {int(hours)}小时{int(minutes)}分钟{int(seconds)}秒")
            logger.info(f"数据已保存到CSV: {csv_path}")

        else:
            logger.warning("未找到任何论文")

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")


if __name__ == "__main__":
    main()
