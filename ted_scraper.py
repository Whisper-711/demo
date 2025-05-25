import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

os.makedirs('data', exist_ok=True)
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

base_url = "https://ted.europa.eu/en/search/result"
params = {
    'classification-cpv': '44000000,45000000',
    'search-scope': 'ALL',
    'page': 1
}

results = []

for i in range(10):
    print(f"抓取第{i+1}页")
    params['page'] = i + 1
    
    response = requests.get(base_url, params=params, headers=headers)
    if response.status_code != 200:
        print(f"请求失败，状态码: {response.status_code}")
        break
    
    soup = BeautifulSoup(response.text, 'html.parser')
    trs = soup.select('tbody tr')
    
    if not trs:
        break
    
    for tr in trs:
        tds = tr.select('td')
        if len(tds) < 4:
            continue
        
        date = tds[0].text.strip()
        title_td = tds[1]
        title = title_td.text.strip()
        link_elem = title_td.select_one('a')
        link = link_elem['href'] if link_elem else ''
        if link and not link.startswith('http'):
            link = 'https://ted.europa.eu' + link
        
        cpv = tds[2].text.strip()
        country = tds[3].text.strip()
        
        results.append([title, link, date, cpv, country])
    
    time.sleep(2)

df = pd.DataFrame(results, columns=["Title", "Link", "Date", "CPV", "Country"])
df.to_csv("data/ted_tenders_requests.csv", index=False, encoding="utf-8")
print("抓取完成") 
