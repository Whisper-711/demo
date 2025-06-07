import requests
import json
import pandas as pd
import os
import time
import logging
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import re


class TedDetailCrawler:
    def __init__(self, input_file=None, output_dir='data', max_details=50, use_cache=True):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('ted_detail_crawler.log', encoding='utf-8')
            ]
        )
        self.logger = logging.getLogger('ted_detail_crawler')
        
        self.output_dir = output_dir
        self.cache_dir = os.path.join(output_dir, 'detail_cache')
        self.input_file = input_file or os.path.join(output_dir, 'ted_api_tenders.csv')
        self.output_file = os.path.join(output_dir, 'ted_tender_details.csv')
        
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.max_details = max_details
        self.use_cache = use_cache
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://ted.europa.eu/'
        })

    def get_cache_file_path(self, notice_number):
        safe_notice = re.sub(r'[\\/*?:"<>|]', "_", notice_number)
        return Path(self.cache_dir) / f'ted_detail_{safe_notice}.html'

    def load_from_cache(self, notice_number):
        cache_file = self.get_cache_file_path(notice_number)
        if cache_file.exists():
            try:
                self.logger.info(f"Loading detail from cache for notice {notice_number}")
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                self.logger.error(f"Error loading cache for notice {notice_number}: {str(e)}")
        return None

    def save_to_cache(self, content, notice_number):
        if not content:
            return
        
        cache_file = self.get_cache_file_path(notice_number)
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(content)
            self.logger.info(f"Saved detail to cache: {cache_file}")
        except Exception as e:
            self.logger.error(f"Error saving cache for notice {notice_number}: {str(e)}")

    def load_tender_list(self):
        if not os.path.exists(self.input_file):
            self.logger.error(f"Input file not found: {self.input_file}")
            return []
        
        try:
            df = pd.read_csv(self.input_file)
            self.logger.info(f"Loaded {len(df)} tenders from {self.input_file}")
            return df.to_dict('records')
        except Exception as e:
            self.logger.error(f"Error loading tender list: {str(e)}")
            return []

    def normalize_url(self, url, notice_number):
        if "/en/notice/-/detail/" in url:
            return url
            
        match = re.search(r'(\d+)-(\d+)', notice_number)
        if match:
            notice_id = f"{match.group(1)}-{match.group(2)}"
            return f"https://ted.europa.eu/en/notice/-/detail/{notice_id}"
            
        match = re.search(r'notice/(\d+)', url)
        if match:
            notice_id = match.group(1)
            return f"https://ted.europa.eu/en/notice/-/detail/{notice_id}"
            
        self.logger.warning(f"Could not normalize URL: {url}")
        return url

    def fetch_detail_page(self, url, notice_number):
        if not url:
            self.logger.warning(f"No URL provided for notice {notice_number}")
            return None
        
        normalized_url = self.normalize_url(url, notice_number)
        
        if self.use_cache:
            cached_content = self.load_from_cache(notice_number)
            if cached_content:
                return cached_content
        
        try:
            self.logger.info(f"Fetching detail page for notice {notice_number}: {normalized_url}")
            response = self.session.get(normalized_url)
            
            if response.status_code == 200:
                content = response.text
                self.logger.info(f"Successfully retrieved detail page for notice {notice_number}")
                
                self.save_to_cache(content, notice_number)
                
                return content
            else:
                self.logger.error(f"Request failed with status code: {response.status_code}")
                return None
        except Exception as e:
            self.logger.error(f"Request exception: {str(e)}")
            return None

    def extract_detail_info(self, html_content, base_info):
        if not html_content:
            return base_info
        
        detail_info = base_info.copy()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            self._extract_by_structure(soup, detail_info)
            
            return detail_info
            
        except Exception as e:
            self.logger.error(f"Error parsing HTML: {str(e)}")
            return base_info

    def _extract_by_structure(self, soup, detail_info):
        title_element = soup.select_one('h1.notice-title') or soup.select_one('h1')
        if title_element:
            detail_info['page_title'] = title_element.get_text(strip=True)
        
        sections = soup.select('section') or soup.select('div.section') or soup.select('div[class*="section"]')
        
        if not sections:
            self.logger.warning("No sections found, trying to extract from divs")
            sections = soup.select('div.row') or soup.select('div[class*="content"]')
        
        for section in sections:
            header = section.select_one('h2, h3, h4, .section-title, .title')
            if header:
                section_title = header.get_text(strip=True)
                self._process_section_by_title(section, section_title, detail_info)
            else:
                self._process_section_by_content(section, detail_info)
        
        self._extract_documents(soup, detail_info)
        
        self._extract_cpv_codes(soup, detail_info)
        
        self._extract_nuts_codes(soup, detail_info)

    def _process_section_by_title(self, section, title, detail_info):
        title_lower = title.lower()
        content = section.get_text(strip=True).replace(title, '', 1).strip()
        
        if any(kw in title_lower for kw in ['contracting authority', 'buyer', 'purchaser']):
            detail_info['buyer_details'] = content
            
            contact_div = section.select_one('div[class*="contact"]')
            if contact_div:
                detail_info['buyer_contact'] = contact_div.get_text(strip=True)
            
            address_div = section.select_one('div[class*="address"]')
            if address_div:
                detail_info['buyer_address'] = address_div.get_text(strip=True)
            
        elif any(kw in title_lower for kw in ['object', 'description', 'subject']):
            detail_info['tender_description'] = content
            
        elif any(kw in title_lower for kw in ['value', 'budget', 'price']):
            detail_info['budget'] = content
            
        elif any(kw in title_lower for kw in ['duration', 'period', 'term']):
            detail_info['duration'] = content
            
        elif any(kw in title_lower for kw in ['award', 'criteria']):
            detail_info['award_criteria'] = content
            
        elif any(kw in title_lower for kw in ['condition', 'requirement']):
            detail_info['conditions'] = content
            
        elif any(kw in title_lower for kw in ['procedure', 'process']):
            detail_info['procedure'] = content
            
        elif any(kw in title_lower for kw in ['deadline', 'date', 'time']):
            detail_info['deadlines'] = content
            
        elif any(kw in title_lower for kw in ['additional', 'other', 'further']):
            detail_info['additional_info'] = content
            
        else:
            safe_key = re.sub(r'[^a-zA-Z0-9_]', '_', title_lower)
            safe_key = re.sub(r'_+', '_', safe_key).strip('_')
            if safe_key and content:
                detail_info[f'section_{safe_key}'] = content

    def _process_section_by_content(self, section, detail_info):
        section_class = section.get('class', [])
        section_class = ' '.join(section_class) if isinstance(section_class, list) else section_class
        section_text = section.get_text(strip=True)
        
        if 'buyer' in section_class or 'authority' in section_class:
            detail_info['buyer_details'] = section_text
        elif 'object' in section_class or 'description' in section_class:
            detail_info['tender_description'] = section_text
        elif 'value' in section_class or 'budget' in section_class:
            detail_info['budget'] = section_text
        elif 'document' in section_class:
            pass

    def _extract_documents(self, soup, detail_info):
        document_links = (
            soup.select('a[href*="document"]') or 
            soup.select('a[class*="document"]') or 
            soup.select('a[href$=".pdf"]') or 
            soup.select('a[href$=".doc"]') or 
            soup.select('a[href$=".docx"]') or 
            soup.select('a[href$=".zip"]')
        )
        
        if document_links:
            docs = []
            for link in document_links:
                url = link.get('href')
                if url and not url.startswith(('http://', 'https://')):
                    url = f"https://ted.europa.eu{url}" if url.startswith('/') else f"https://ted.europa.eu/{url}"
                
                title = link.get_text(strip=True) or os.path.basename(url)
                if url:
                    docs.append({'title': title, 'url': url})
            
            if docs:
                detail_info['documents'] = json.dumps(docs, ensure_ascii=False)

    def _extract_cpv_codes(self, soup, detail_info):
        cpv_elements = (
            soup.select('[class*="cpv"]') or 
            soup.select('div:has(> span:contains("CPV"))') or
            soup.select('tr:has(> td:contains("CPV"))')
        )
        
        if cpv_elements:
            cpv_texts = []
            for elem in cpv_elements:
                text = elem.get_text(strip=True)
                if 'CPV' in text:
                    cpv_texts.append(text)
            
            if cpv_texts:
                detail_info['cpv_codes'] = ', '.join(cpv_texts)

    def _extract_nuts_codes(self, soup, detail_info):
        nuts_elements = (
            soup.select('[class*="nuts"]') or 
            soup.select('div:has(> span:contains("NUTS"))') or
            soup.select('tr:has(> td:contains("NUTS"))')
        )
        
        if nuts_elements:
            nuts_texts = []
            for elem in nuts_elements:
                text = elem.get_text(strip=True)
                if 'NUTS' in text:
                    nuts_texts.append(text)
            
            if nuts_texts:
                detail_info['nuts_codes'] = ', '.join(nuts_texts)

    def save_data(self, data, filename=None, append=False):
        if not data:
            return
            
        if filename is None:
            filename = self.output_file
            
        df = pd.DataFrame([data])
        
        mode = 'a' if append else 'w'
        header = not (append and os.path.exists(filename))

        df.to_csv(filename, mode=mode, header=header, index=False, encoding='utf-8-sig')
        self.logger.info(f"Saved detail data for notice {data.get('notice_number', 'unknown')} to {filename}")

    def run(self):
        tenders = self.load_tender_list()
        if not tenders:
            self.logger.error("No tenders to process")
            return []
        
        if self.max_details > 0 and len(tenders) > self.max_details:
            self.logger.info(f"Limiting to {self.max_details} tenders out of {len(tenders)}")
            tenders = tenders[:self.max_details]
        
        self.logger.info(f"Starting to fetch details for {len(tenders)} tenders...")
        
        start_time = time.time()
        processed_count = 0
        success_count = 0
        
        for i, tender in enumerate(tenders):
            notice_number = tender.get('notice_number')
            link = tender.get('link')
            
            if not notice_number or not link:
                self.logger.warning(f"Missing notice number or link for tender {i+1}")
                continue
            
            self.logger.info(f"Processing tender {i+1}/{len(tenders)}: {notice_number}")
            
            html_content = self.fetch_detail_page(link, notice_number)
            
            if html_content:
                detail_info = self.extract_detail_info(html_content, tender)
                
                self.save_data(detail_info, append=(i > 0))
                
                success_count += 1
            
            processed_count += 1
            
            if i < len(tenders) - 1:
                delay = 2.0
                self.logger.info(f"Waiting {delay} seconds before next request")
                time.sleep(delay)
        
        end_time = time.time()
        self.logger.info(f"\nDetail crawling completed")
        self.logger.info(f"Processed {processed_count} tenders, successfully fetched {success_count} details")
        self.logger.info(f"Data has been saved to: {self.output_file}")
        self.logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
        
        return processed_count, success_count

    def download_documents(self, output_dir=None):
        if output_dir is None:
            output_dir = os.path.join(self.output_dir, 'documents')
        
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            df = pd.read_csv(self.output_file)
        except Exception as e:
            self.logger.error(f"Error loading detail data: {str(e)}")
            return 0
        
        download_count = 0
        
        for _, row in df.iterrows():
            notice_number = row.get('notice_number')
            documents_json = row.get('documents')
            
            if not documents_json or pd.isna(documents_json):
                continue
            
            try:
                documents = json.loads(documents_json)
                
                for i, doc in enumerate(documents):
                    doc_url = doc.get('url')
                    doc_title = doc.get('title', f'document_{i}')
                    
                    if not doc_url:
                        continue
                    
                    safe_title = re.sub(r'[\\/*?:"<>|]', "_", doc_title)
                    file_name = f"{notice_number}_{safe_title[:50]}.pdf"
                    file_path = os.path.join(output_dir, file_name)
                    
                    if os.path.exists(file_path):
                        self.logger.info(f"Document already exists: {file_path}")
                        download_count += 1
                        continue
                    
                    try:
                        self.logger.info(f"Downloading document: {doc_title}")
                        response = self.session.get(doc_url, stream=True)
                        
                        if response.status_code == 200:
                            with open(file_path, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            
                            self.logger.info(f"Document saved to: {file_path}")
                            download_count += 1
                        else:
                            self.logger.error(f"Failed to download document: {response.status_code}")
                    
                    except Exception as e:
                        self.logger.error(f"Error downloading document: {str(e)}")
                    
                    time.sleep(1.0)
            
            except Exception as e:
                self.logger.error(f"Error processing documents for {notice_number}: {str(e)}")
        
        self.logger.info(f"Downloaded {download_count} documents to {output_dir}")
        return download_count


def main():
    crawler = TedDetailCrawler(
        input_file='data/ted_api_tenders.csv',
        output_dir='data',
        max_details=10,
        use_cache=True
    )
    
    processed, success = crawler.run()
    
    return processed, success


if __name__ == "__main__":
    main() 
