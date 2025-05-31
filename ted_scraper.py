import requests
import json
import pandas as pd
import os
import time
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ted_crawler.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('ted_crawler')

OUTPUT_DIR = 'data'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'ted_api_tenders_10pages.csv')
CACHE_DIR = os.path.join(OUTPUT_DIR, 'cache')
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

API_URL = 'https://tedweb.api.ted.europa.eu/private-search/api/v1/notices/search'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Origin': 'https://ted.europa.eu',
    'Referer': 'https://ted.europa.eu/'
}

def create_payload(page_number=1, page_size=50):
    return {
        "query": "(classification-cpv IN (44000000 45000000))  SORT BY publication-number DESC",
        "page": page_number,
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

def get_cache_file_path(page_number):
    return Path(CACHE_DIR) / f'ted_api_raw_page{page_number}.json'

def load_from_cache(page_number):
    cache_file = get_cache_file_path(page_number)
    if cache_file.exists():
        try:
            logger.info(f"Loading data from cache for page {page_number}")
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading cache for page {page_number}: {str(e)}")
    return None

def save_to_cache(data, page_number):
    if not data:
        return
    
    cache_file = get_cache_file_path(page_number)
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved data to cache: {cache_file}")
    except Exception as e:
        logger.error(f"Error saving cache for page {page_number}: {str(e)}")

def fetch_tenders(session, page_number=1, page_size=50, use_cache=True):
    if use_cache:
        cached_data = load_from_cache(page_number)
        if cached_data:
            return cached_data
    
    payload = create_payload(page_number, page_size)
    
    try:
        logger.info(f"Requesting page {page_number} data from API...")
        response = session.post(API_URL, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully retrieved page {page_number} data")
            
            save_to_cache(data, page_number)
            
            return data
        else:
            logger.error(f"Request failed with status code: {response.status_code}")
            logger.error(f"Response content: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Request exception: {str(e)}")
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
    logger.info(f"Saved {len(df)} records to {filename}")

def scrape_ted_api(max_pages=10, use_cache=True):
    all_tenders = []
    total_count = 0
    
    logger.info(f"Starting TED API data scraping, planning to scrape {max_pages} pages...")
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    for page_number in range(1, max_pages + 1):
        logger.info(f"\nScraping page {page_number}...")
        
        data = fetch_tenders(session, page_number, use_cache=use_cache)
        
        if not data:
            logger.error(f"Failed to get data for page {page_number}, stopping scraping")
            break
        
        notices = data.get('notices', [])
        
        if not notices:
            logger.warning(f"No notice data on page {page_number}, stopping scraping")
            break
        
        if page_number == 1 and 'totalNoticeCount' in data:
            total_count = data.get('totalNoticeCount', 0)
            logger.info(f"Found a total of {total_count} tender notices")
        
        page_tenders = []
        for notice in notices:
            tender = extract_tender_info(notice)
            page_tenders.append(tender)
        
        logger.info(f"Extracted {len(page_tenders)} records from page {page_number}")
        
        all_tenders.extend(page_tenders)
        
        save_data(page_tenders, OUTPUT_FILE, append=(page_number > 1))
        
        if page_number < max_pages:
            delay = 2.0
            logger.info(f"Waiting {delay} seconds before scraping the next page")
            time.sleep(delay)
    
    logger.info(f"\nScraping completed, scraped a total of {len(all_tenders)} records")
    return all_tenders

if __name__ == "__main__":
    MAX_PAGES = 10
    USE_CACHE = True
    
    start_time = time.time()
    tenders = scrape_ted_api(MAX_PAGES, USE_CACHE)
    end_time = time.time()
    
    logger.info(f"Data has been saved to: {OUTPUT_FILE}")
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
