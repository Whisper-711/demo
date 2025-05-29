import csv
import time
import random
import os
import traceback
from DrissionPage import ChromiumPage

BASE_URL = 'https://ted.europa.eu/en/search/result?classification-cpv=44000000%2C45000000&search-scope=ALL&only-latest-versions=true'

MAX_PAGES = 15

OUTPUT_FILE = 'data/ted_tenders_all.csv'

RESUME_FROM_LAST = True

def get_page_url(page_number):
    if page_number == 1:
        return BASE_URL
    return f"{BASE_URL}&page={page_number}"

def wait_for_table_load(page, max_wait=30):
    start_time = time.time()
    while time.time() - start_time < max_wait:
        rows1 = page.eles('xpath://tr[contains(@class, "MuiTableRow-root")]', timeout=1)
        rows2 = page.eles('xpath://table//tr', timeout=1)
        
        if rows1 or rows2:
            return True
        
        time.sleep(2)
    return False

def analyze_page_structure(page):
    try:
        tables = page.eles('tag:table')
    except Exception:
        pass

def extract_data_from_table(page):
    data = []
    
    selectors = [
        '//tr[contains(@class, "MuiTableRow-root") and not(contains(@class, "MuiTableRow-head"))]',
        '//tr[./td[contains(@class, "MuiTableCell")]]',
        '//table//tr[position()>1]',
        '//div[contains(@class, "MuiTable")]//tr[position()>1]',
        '//tr[.//a[contains(@href, "notice")]]'
    ]
    
    rows = []
    for selector in selectors:
        try:
            found_rows = page.eles(f'xpath:{selector}')
            if found_rows:
                rows = found_rows
                break
        except Exception:
            continue
    
    if not rows:
        return data
    
    print(f"找到 {len(rows)} 行数据")
    
    for i, row in enumerate(rows):
        try:
            cells = row.eles('xpath:.//td') or row.eles('tag:td') or row.eles('xpath:./td')
            
            if len(cells) < 3:  
                notice_link = row.ele('tag:a', timeout=1)
                if notice_link:
                    notice_number = notice_link.text.strip()
                    data.append({
                        'notice_number': notice_number,
                        'description': "无法提取描述",
                        'country': "无法提取国家",
                        'publication_date': "",
                        'deadline': "",
                        'page_number': current_page
                    })
                continue
            
            notice_number = ""
            description = ""
            country = ""
            pub_date = ""
            deadline = ""
            
            if len(cells) > 1:
                cell_text = cells[1].text.strip() if cells[1].text else ""
                notice_link = cells[1].ele('tag:a', timeout=1)
                if notice_link and notice_link.text:
                    notice_number = notice_link.text.strip()
                    
                    description = cell_text.replace(notice_number, '').strip()
                    if description.startswith('...'):
                        description = description[3:].strip()
                else:
                    parts = cell_text.split('...')
                    if len(parts) > 0:
                        notice_number = parts[0].strip()
                    
                    if len(parts) > 1:
                        description = ''.join(parts[1:]).strip()
                    else:
                        lines = cell_text.split('\n')
                        if len(lines) > 1:
                            description = '\n'.join(lines[1:]).strip()
                        else:
                            description = cell_text
            
            if len(cells) > 2:
                country = cells[2].text.strip() if cells[2].text else ""
            
            if len(cells) > 3:
                pub_date = cells[3].text.strip() if cells[3].text else ""
            
            if len(cells) > 4:
                deadline = cells[4].text.strip() if cells[4].text else ""
            
            tender_data = {
                'notice_number': notice_number.replace('"', '""'),
                'description': description.replace('"', '""'),
                'country': country.replace('"', '""'),
                'publication_date': pub_date.replace('"', '""'),
                'deadline': deadline.replace('"', '""'),
                'page_number': current_page
            }
            
            if notice_number or description or country:
                data.append(tender_data)
        except Exception:
            continue
    
    return data

def save_to_csv(data, filename=OUTPUT_FILE, append=False):
    if not data:
        return
        
    mode = 'a' if append else 'w'
    exists = os.path.exists(filename)
    
    with open(filename, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        if mode == 'w' or (mode == 'a' and not exists):
            writer.writeheader()
        writer.writerows(data)

def get_last_page_scraped():
    if not os.path.exists(OUTPUT_FILE):
        return 0
    
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            pages = [int(row.get('page_number', 0)) for row in reader if row.get('page_number')]
            return max(pages) if pages else 0
    except Exception:
        return 0

def main():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    start_page = 1
    if RESUME_FROM_LAST:
        last_page = get_last_page_scraped()
        if last_page > 0:
            print(f"从第 {last_page + 1} 页继续")
            start_page = last_page + 1
    
    if start_page > MAX_PAGES:
        print(f"已抓取完所有 {MAX_PAGES} 页数据")
        return
    
    
    try:
        page = ChromiumPage(timeout=60)
        
        if not RESUME_FROM_LAST or not os.path.exists(OUTPUT_FILE):
            if os.path.exists(OUTPUT_FILE):
                os.remove(OUTPUT_FILE)
        
        total_records = 0
        
        global current_page
        
        for current_page in range(start_page, MAX_PAGES + 1):
            print(f"\n正在抓取第 {current_page} 页")
            
            page_url = get_page_url(current_page)
            
            max_retries = 5
            success = False
            for retry in range(max_retries):
                try:
                    print(f"加载页面 (尝试 {retry+1}/{max_retries})...")
                    page.get(page_url)
                    success = True
                    break
                except Exception:
                    if retry == max_retries - 1:
                        break
                    
                    wait_time = (retry + 1) * 10
                    time.sleep(wait_time)
                    
                    try:
                        page.quit()
                        page = ChromiumPage(timeout=60)
                    except:
                        pass
            
            if not success:
                print(f"无法加载第 {current_page} 页")
                continue
            
            time.sleep(15)
            
            if current_page == 1 or retry > 0:
                try:
                    cookie_button = page.ele('xpath://button[contains(text(), "Accept") or contains(text(), "同意")]', timeout=3)
                    if cookie_button:
                        cookie_button.click()
                        time.sleep(3)
                except:
                    pass
            
            wait_for_table_load(page)
            
            try:
                page_data = extract_data_from_table(page)
            except Exception:
                continue
            
            if not page_data:
                print(f"第 {current_page} 页未找到数据")
                try:
                    page.get(page_url)
                    time.sleep(15)
                    page_data = extract_data_from_table(page)
                except Exception:
                    pass
                
                if not page_data:
                    print(f"第 {current_page} 页重试后仍未找到数据")
                    continue
            
            # 保存数据
            try:
                save_to_csv(page_data, append=(current_page > start_page or start_page > 1))
                total_records += len(page_data)
                print(f"第 {current_page} 页已保存 ({len(page_data)}条)")
                print(f"累计已保存 {total_records} 条记录")
            except Exception as e:
                print(f"保存数据时出错: {str(e)}")
            
            delay = random.uniform(5.0, 10.0)
            time.sleep(delay)
        
        print(f"\n抓取完成，共保存 {total_records} 条记录")
        
    except Exception as e:
        print(f"抓取过程出错: {str(e)}")
    finally:
        try:
            page.quit()
        except:
            pass

if __name__ == '__main__':
    main()
