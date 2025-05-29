import requests
import json
import pandas as pd
import os
import time
from datetime import datetime

OUTPUT_DIR = 'data'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'ted_api_tenders_10pages.csv')
os.makedirs(OUTPUT_DIR, exist_ok=True)

API_URL = 'https://tedweb.api.ted.europa.eu/private-search/api/v1/notices/search'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Origin': 'https://ted.europa.eu',
    'Referer': 'https://ted.europa.eu/'
}

def create_payload(page=1, page_size=50):
    return {
        "query": "(classification-cpv IN (44000000 45000000))  SORT BY publication-number DESC",
        "page": page,
        "limit": page_size,
        "fields": [
            "publication-number",
            "BT-5141-Procedure",
            "BT-5141-Part",
            "BT-5141-Lot",
            "BT-5071-Procedure",
            "BT-5071-Part",
            "BT-5071-Lot",
            "BT-727-Procedure",
            "BT-727-Part",
            "BT-727-Lot",
            "place-of-performance",
            "procedure-type",
            "contract-nature",
            "buyer-name",
            "buyer-country",
            "publication-date",
            "deadline-receipt-request",
            "notice-title",
            "official-language",
            "notice-type",
            "change-notice-version-identifier"
        ],
        "validation": False,
        "scope": "ALL",
        "language": "EN",
        "onlyLatestVersions": True,
        "facets": {
            "business-opportunity": [],
            "cpv": [],
            "contract-nature": [],
            "place-of-performance": [],
            "procedure-type": [],
            "publication-date": [],
            "buyer-country": []
        }
    }

def fetch_tenders(page=1, page_size=50):
    """从API获取招标数据"""
    payload = create_payload(page, page_size)
    
    try:
        print(f"正在请求第 {page} 页数据...")
        response = requests.post(API_URL, headers=HEADERS, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            print(f"成功获取第 {page} 页数据")
            return data
        else:
            print(f"请求失败，状态码: {response.status_code}")
            print(f"响应内容: {response.text}")
            return None
    except Exception as e:
        print(f"请求异常: {str(e)}")
        return None

def extract_tender_info(notice):
    tender = {}
    
    tender['notice_number'] = notice.get('publication-number', '')
    
    notice_type = notice.get('notice-type', {})
    tender['notice_type'] = notice_type.get('label', '') if notice_type else ''
    
    buyer_name = notice.get('buyer-name', {})
    if buyer_name:
        for lang, names in buyer_name.items():
            if names and len(names) > 0:
                tender['buyer_name'] = names[0]
                break
    else:
        tender['buyer_name'] = ''
    
    buyer_country = notice.get('buyer-country', [])
    tender['buyer_country'] = buyer_country[0].get('label', '') if buyer_country and len(buyer_country) > 0 else ''
    
    contract_nature = notice.get('contract-nature', [])
    tender['contract_nature'] = contract_nature[0].get('label', '') if contract_nature and len(contract_nature) > 0 else ''
    
    tender['publication_date'] = notice.get('publication-date', '')
    
    deadline = notice.get('deadline-receipt-request', [])
    tender['deadline'] = deadline[0] if deadline and len(deadline) > 0 else ''
    
    notice_title = notice.get('notice-title', {})
    if notice_title:
        tender['title'] = notice_title.get('eng', '')
        if not tender['title']:
            for lang, title in notice_title.items():
                if title:
                    tender['title'] = title
                    break
    else:
        tender['title'] = ''
    
    links = notice.get('links', {})
    if links:
        html_links = links.get('html', {})
        if html_links and 'ENG' in html_links:
            tender['link'] = html_links.get('ENG', '')
        elif html_links:
            for lang, link in html_links.items():
                if link:
                    tender['link'] = link
                    break
        else:
            tender['link'] = ''
    else:
        tender['link'] = ''
    
    place_of_performance = notice.get('place-of-performance', [])
    if place_of_performance and len(place_of_performance) > 0:
        places = [place.get('label', '') for place in place_of_performance if place.get('label')]
        tender['place_of_performance'] = ', '.join(places)
    else:
        tender['place_of_performance'] = ''
    
    return tender

def save_data(data, filename, append=False):
    if not data:
        return
        
    df = pd.DataFrame(data)
    
    mode = 'a' if append else 'w'
    header = not (append and os.path.exists(filename))

    df.to_csv(filename, mode=mode, header=header, index=False, encoding='utf-8-sig')
    print(f"保存了 {len(df)} 条数据到 {filename}")

def save_raw_data(data, page):
    if not data:
        return
    
    raw_file = os.path.join(OUTPUT_DIR, f'ted_api_raw_page{page}.json')
    with open(raw_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"保存数据到 {raw_file}")

def scrape_ted_api(max_pages=10):
    all_tenders = []
    total_count = 0
    
    print(f"开始抓取数据，计划抓取 {max_pages} 页...")
    
    for page in range(1, max_pages + 1):
        print(f"\n正在抓取第 {page} 页...")
        
        data = fetch_tenders(page)
        
        if not data:
            print(f"第 {page} 页数据获取失败，停止抓取")
            break
        
        save_raw_data(data, page)
        
        notices = data.get('notices', [])
        
        if not notices:
            print(f"第 {page} 页没有通知数据，停止抓取")
            break
        
        if page == 1 and 'totalNoticeCount' in data:
            total_count = data.get('totalNoticeCount', 0)
            print(f"总共找到 {total_count} 条")
        
        page_tenders = []
        for notice in notices:
            tender = extract_tender_info(notice)
            page_tenders.append(tender)
        
        print(f"从第 {page} 页提取了 {len(page_tenders)} 条数据")
        
        all_tenders.extend(page_tenders)
        
        save_data(page_tenders, OUTPUT_FILE, append=(page > 1))
        
        if page < max_pages:
            delay = 2.0
            print(f"等待 {delay} 秒后抓取下一页")
            time.sleep(delay)
    
    print(f"\n抓取完成，共抓取 {len(all_tenders)} 条数据")
    return all_tenders

if __name__ == "__main__":
    MAX_PAGES = 10
    
    start_time = time.time()
    tenders = scrape_ted_api(MAX_PAGES)
    end_time = time.time()
    
    print(f"数据已保存到: {OUTPUT_FILE}") 
