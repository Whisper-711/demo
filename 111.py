import os
import requests
import time
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


if not os.path.exists('data'):
    os.makedirs('data')

retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

# 禁用代理设置
session.proxies = {
    'http': None,
    'https': None
}


base_url = "https://www.biorxiv.org/search/visium%20numresults%3A75%20sort%3Arelevance-rank"


for page_num in range(5):
    url = f"{base_url}?page={page_num}"

    print(f"正在爬取第 {page_num + 1} 页: {url}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = session.get(url, headers=headers, timeout=30, verify=False)
            response.raise_for_status()

            file_path = os.path.join('data', f'biorxiv_visium_page_{page_num}.html')
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)

            print(f"保存第 {page_num + 1} 页内容到 {file_path}")
            break

        except Exception as e:
            print(f"尝试 {attempt + 1}/{max_retries}: 爬取第 {page_num + 1} 页时发生错误: {e}")
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)
                print(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print(f"已达最大重试次数，跳过第 {page_num + 1} 页")


    if page_num < 4:
        wait_time = 5
        print(f"等待 {wait_time} 秒...")
        time.sleep(wait_time)

print("爬取完成！")

