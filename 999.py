import requests
from bs4 import BeautifulSoup
import json
import datetime
import re
import math
import pandas as pd
import os
from urllib import parse
import time

# 设置日志
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BioScraper")

class BioScraper:
    def __init__(self):
        # 初始化请求会话
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7'
        })
        
        # 运行日期
        self.run_date = datetime.datetime.now()
        
        # 存储抓取的数据
        self.papers = []
        
        # 确保输出目录存在
        os.makedirs("data", exist_ok=True)

    def download_page(self, url, retry=3):
        """下载网页内容"""
        for attempt in range(retry):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except Exception as e:
                logger.error(f"下载失败 {url}: {e}")
                if attempt < retry - 1:
                    time.sleep(2)
                else:
                    raise
    
    def get_publish_text_templates(self):
        """获取发布文本模板"""
        text_url = 'https://d33xdlntwy0kbs.cloudfront.net/cshl_custom.js'
        templates = {}
        
        try:
            content = self.download_page(text_url)
            soup = BeautifulSoup(content, 'html.parser')
            text_elements = soup.select('div.pub_jnl')
            
            for element in text_elements:
                text = element.text
                if text.startswith('This '):
                    templates['None'] = text
                else:
                    match = re.search(r'Now (\w+) ', text)
                    if match:
                        pub_type = match.group(1)
                        templates[pub_type] = text
            
            return templates
        except Exception as e:
            logger.error(f"获取发布模板失败: {e}")
            return {'None': 'This article has not been published yet.'}
    
    def get_posted_time(self, doi):
        """获取论文发布时间"""
        url = f'https://www.biorxiv.org/content/{doi}v1'
        try:
            page = self.download_page(url)
            soup = BeautifulSoup(page, 'html.parser')
            time_element = soup.select_one('div.pane-1 div.pane-content')
            if time_element:
                return time_element.text.strip('\n Posted')
            return ""
        except Exception as e:
            logger.error(f"获取发布时间失败: {e}")
            return ""
    
    def get_paper_status(self, doi, templates):
        """获取论文发布状态"""
        url = f'https://connect.biorxiv.org/bx_pub_doi_get.php?doi={doi}'
        try:
            content = self.download_page(url)
            # 清理JSON字符串
            content = content.replace('\r\n', '').replace('\n', '').replace('(', '').replace(')', '')
            
            data = json.loads(content)
            if not data.get('pub') or len(data['pub']) == 0:
                return 'None'
            
            pub_info = data['pub'][0]
            pub_type = pub_info.get('pub_type')
            pub_doi = pub_info.get('pub_doi', '')
            pub_journal = pub_info.get('pub_journal', '')
            
            if not pub_type:
                return 'None'
            
            # 获取模板并替换变量
            template = templates.get(pub_type, templates.get('None', 'None'))
            result = template.replace("'+y[B].pubjournal+'", pub_journal).replace(
                      '+y[B].pubdoi+"', pub_doi)
            
            return result.replace("\'", "").replace("'", "")
        except Exception as e:
            logger.error(f"获取论文状态失败: {e}")
            return 'None'
    
    def scrape(self, keywords, max_pages=None):
        """抓取论文数据"""
        templates = self.get_publish_text_templates()
        timestamp = self.run_date.strftime("%Y-%m-%d %H:%M:%S")
        
        for keyword in keywords:
            logger.info(f"抓取关键词: {keyword}")
            page = 0
            total_pages = 1
            
            while page < total_pages:
                url = f'https://www.biorxiv.org/search/{parse.quote(keyword)}%20numresults%3A75%20sort%3Arelevance-rank?page={page}'
                logger.info(f"处理页面 {page + 1}")
                
                try:
                    content = self.download_page(url)
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # 获取总结果数
                    summary = soup.select_one('div.highwire-search-summary')
                    if summary:
                        match = re.search(r'([\d,]+)\sResults', summary.text)
                        if match:
                            total_results = int(match.group(1).replace(",", ""))
                            total_pages = math.ceil(total_results / 75)
                            
                            if max_pages and max_pages < total_pages:
                                total_pages = max_pages
                    
                    # 处理文章列表
                    articles = soup.select(".highwire-search-results-list > li")
                    logger.info(f"找到 {len(articles)} 篇文章")
                    
                    for article in articles:
                        try:
                            paper = {}
                            
                            # 提取标题
                            title_element = article.select_one(".highwire-cite-title")
                            if title_element:
                                paper["title"] = title_element.text
                            
                            # 提取作者
                            authors_element = article.select_one(".highwire-cite-authors")
                            if authors_element:
                                paper["authors"] = authors_element.text
                            
                            # 提取DOI信息
                            date_id = article.select_one(".highwire-cite-metadata-pages")
                            doi_element = article.select_one(".doi_label")
                            
                            if doi_element and doi_element.next_sibling and date_id:
                                doi_text = doi_element.next_sibling
                                doi = doi_text.replace('https://doi.org/', '').replace(' ', '')
                                
                                paper["search_keyword"] = keyword
                                paper["source_url"] = url
                                paper["detail_url"] = f'https://www.biorxiv.org/content/{doi}v1'
                                
                                # 处理日期和ID
                                date_id_text = date_id.text.split(";")[0]
                                if "." in date_id_text:
                                    paper["doi_ID"] = date_id_text.split(".")[-1]
                                    date_raw = date_id_text.rsplit('.', 1)[0]
                                    paper["doi_date"] = str(datetime.datetime.strptime(date_raw, '%Y.%m.%d').date())
                                else:
                                    paper["doi_ID"] = date_id_text
                                    paper["doi_date"] = None
                                
                                # 获取发布时间
                                posted_time = self.get_posted_time(doi)
                                if 'Youarenotauthorizedtoaccessthispage' not in posted_time:
                                    paper["posted_time_raw"] = posted_time
                                    time_cleaned = posted_time.replace(' ', '').replace('\xa0', '').strip('.')
                                    try:
                                        time_obj = datetime.datetime.strptime(time_cleaned, '%B%d,%Y')
                                        paper["posted_time"] = time_obj.strftime('%Y-%m-%d %H:%M:%S')
                                    except:
                                        paper["posted_time"] = None
                                
                                # 获取发布状态
                                paper["publish_text"] = self.get_paper_status(doi, templates)
                                
                                # 添加运行信息
                                paper["run_id"] = timestamp
                                paper["run_date"] = timestamp
                                paper["insert_update_time"] = timestamp
                                
                                self.papers.append(paper)
                                logger.info(f"处理文章: {paper.get('title', '未知标题')}")
                            
                        except Exception as e:
                            logger.error(f"处理文章时出错: {e}")
                    
                    page += 1
                    time.sleep(2)  # 添加延迟避免请求过快
                    
                except Exception as e:
                    logger.error(f"处理页面时出错: {e}")
                    break
        
        return self.papers
    
    def save_to_csv(self):
        """保存数据到CSV"""
        if not self.papers:
            logger.warning("没有数据可保存")
            return None
        
        filename = f"data/biorxiv_{self.run_date.strftime('%Y-%m-%d')}.csv"
        df = pd.DataFrame(self.papers)
        df.to_csv(filename, index=False)
        logger.info(f"保存数据到 {filename}, 共 {len(self.papers)} 篇论文")
        return filename

def main():
    # 创建爬虫
    scraper = BioScraper()
    
    # 设置关键词
    keywords = ['visium', '"10x" chromium']
    
    # 开始抓取
    logger.info("开始抓取论文数据")
    scraper.scrape(keywords)
    
    # 保存结果
    scraper.save_to_csv()
    logger.info("抓取完成")

if __name__ == "__main__":
    main()
