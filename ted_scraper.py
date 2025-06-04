import requests
import json
import pandas as pd
import os
import time
import logging
from datetime import datetime
from pathlib import Path


class TedCrawler:
    def __init__(self, output_dir='data', max_pages=10, use_cache=True):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('ted_crawler.log', encoding='utf-8')
            ]
        )
        self.logger = logging.getLogger('ted_crawler')
        
        self.output_dir = output_dir
        self.cache_dir = os.path.join(output_dir, 'cache')
        self.output_file = os.path.join(output_dir, 'ted_api_tenders.csv')
        
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.api_url = 'https://tedweb.api.ted.europa.eu/private-search/api/v1/notices/search'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Origin': 'https://ted.europa.eu',
            'Referer': 'https://ted.europa.eu/'
        }
        
        self.max_pages = max_pages
        self.use_cache = use_cache
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def create_payload(self, page_number=1, page_size=50):
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

    def get_cache_file_path(self, page_number):
        return Path(self.cache_dir) / f'ted_api_raw_page{page_number}.json'

    def load_from_cache(self, page_number):
        cache_file = self.get_cache_file_path(page_number)
        if cache_file.exists():
            try:
                self.logger.info(f"Loading data from cache for page {page_number}")
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading cache for page {page_number}: {str(e)}")
        return None

    def save_to_cache(self, data, page_number):
        if not data:
            return
        
        cache_file = self.get_cache_file_path(page_number)
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Saved data to cache: {cache_file}")
        except Exception as e:
            self.logger.error(f"Error saving cache for page {page_number}: {str(e)}")

    def save_page_content(self, content, page_number, content_type='json'):
        if not content:
            return
            
        file_path = Path(self.cache_dir) / f'ted_page{page_number}.{content_type}'
        try:
            if content_type == 'json':
                with open(file_path, 'w', encoding='utf-8') as f:
                    if isinstance(content, dict) or isinstance(content, list):
                        json.dump(content, f, ensure_ascii=False, indent=2)
                    else:
                        f.write(str(content))
            else:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(str(content))
            self.logger.info(f"Saved page content to: {file_path}")
        except Exception as e:
            self.logger.error(f"Error saving page content for page {page_number}: {str(e)}")

    def fetch_tenders(self, page_number=1, page_size=50):
        if self.use_cache:
            cached_data = self.load_from_cache(page_number)
            if cached_data:
                return cached_data
        
        payload = self.create_payload(page_number, page_size)
        
        try:
            self.logger.info(f"Requesting page {page_number} data from API...")
            response = self.session.post(self.api_url, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                self.logger.info(f"Successfully retrieved page {page_number} data")
                
                # 保存原始响应
                self.save_page_content(data, page_number, 'json')
                # 保存缓存
                self.save_to_cache(data, page_number)
                
                return data
            else:
                self.logger.error(f"Request failed with status code: {response.status_code}")
                self.logger.error(f"Response content: {response.text}")
                return None
        except Exception as e:
            self.logger.error(f"Request exception: {str(e)}")
            return None

    def extract_tender_info(self, notice):
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
            if html_links and 'ENG' in html_links.keys():
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

    def save_data(self, data, filename=None, append=False):
        if not data:
            return
            
        if filename is None:
            filename = self.output_file
            
        df = pd.DataFrame(data)
        
        mode = 'a' if append else 'w'
        header = not (append and os.path.exists(filename))

        df.to_csv(filename, mode=mode, header=header, index=False, encoding='utf-8-sig')
        self.logger.info(f"Saved {len(df)} records to {filename}")

    def run(self):
        all_tenders = []
        total_count = 0
        
        self.logger.info(f"Starting TED API data scraping, planning to scrape {self.max_pages} pages...")
        
        start_time = time.time()
        
        for page_number in range(1, self.max_pages + 1):
            self.logger.info(f"\nScraping page {page_number}...")
            
            data = self.fetch_tenders(page_number)
            
            if not data:
                self.logger.error(f"Failed to get data for page {page_number}, stopping scraping")
                break
            
            notices = data.get('notices', [])
            
            if not notices:
                self.logger.warning(f"No notice data on page {page_number}, stopping scraping")
                break
            
            if page_number == 1 and data.get('totalNoticeCount') is not None:
                total_count = data.get('totalNoticeCount', 0)
                self.logger.info(f"Found a total of {total_count} tender notices")
            
            page_tenders = []
            for notice in notices:
                tender = self.extract_tender_info(notice)
                page_tenders.append(tender)
            
            self.logger.info(f"Extracted {len(page_tenders)} records from page {page_number}")
            
            all_tenders.extend(page_tenders)
            
            self.save_data(page_tenders, append=(page_number > 1))
            
            if page_number < self.max_pages:
                delay = 2.0
                self.logger.info(f"Waiting {delay} seconds before scraping the next page")
                time.sleep(delay)
        
        end_time = time.time()
        self.logger.info(f"\nScraping completed, scraped a total of {len(all_tenders)} records")
        self.logger.info(f"Data has been saved to: {self.output_file}")
        self.logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
        
        return all_tenders


def main():

    crawler = TedCrawler(
        output_dir='data',
        max_pages=10,
        use_cache=True
    )
    tenders = crawler.run()
    return tenders


if __name__ == "__main__":
    main() 
