import requests
import csv
import time
import random

COOKIES = {
    'GUEST_LANGUAGE_ID': 'en_GB',
    'COOKIE_SUPPORT': 'true',
    'cck1': '%7B%22cm%22%3Atrue%2C%22all1st%22%3Atrue%2C%22closed%22%3Afalse%7D',
    'route': '1748006189.756.100.725483|726825d00aba56cccab96f4e82375684'
}

HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'content-type': 'application/json',
    'referer': 'https://ted.europa.eu/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
}

API_URL = 'https://tedweb.api.ted.europa.eu/private-search/api/v1/notices/search'

PAGE_START = 1
PAGE_LIMIT = 50


def build_query(page: int):
    return {
        'query': 'CPV IN ("44000000", "45000000")',
        'page': page,
        'limit': PAGE_LIMIT,
        'fields': [
            'publication-number', 'notice-title', 'buyer-country',
            'publication-date', 'contract-nature', 'procedure-type'
        ],
        'validation': False,
        'scope': 'ACTIVE',
        'language': 'EN',
        'onlyLatestVersions': True,
        'facets': {'business-opportunity': []},
        'sort': [{'publication-date': 'DESC'}]
    }


def fetch_page(page: int):
    try:
        response = requests.post(
            API_URL,
            cookies=COOKIES,
            headers=HEADERS,
            json=build_query(page),
            timeout=15
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"第{page}页请求失败: {str(e)}")
        return None


def save_to_csv(data, filename='ted_tenders.csv'):
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        if f.tell() == 0:
            writer.writeheader()
        writer.writerows(data)


def main():
    first_page = fetch_page(PAGE_START)
    if not first_page:
        return

    total_items = first_page.get('totalElements', 0)
    total_pages = (total_items + PAGE_LIMIT - 1) // PAGE_LIMIT
    print(f"总数据量: {total_items}条 | 总页数: {total_pages}")

    save_to_csv(first_page.get('content', []))
    print(f"第{PAGE_START}页已保存 ({len(first_page.get('content', []))}条)")

    for page in range(PAGE_START + 1, total_pages + 1):
        data = fetch_page(page)
        if not data or 'content' not in data:
            continue

        save_to_csv(data['content'])
        print(f"第{page}页已保存 ({len(data['content'])}条)")



if __name__ == '__main__':
    main()
