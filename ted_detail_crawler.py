import requests
import json
import os
import pandas as pd
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ted_crawler.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('ted_crawler')

# 设置请求 URL 和请求头
url = 'https://tedweb.api.ted.europa.eu/private-search/api/v1/notices/search'

headers = {
    'content-length': '568',
    'sec-ch-ua-platform': '"Windows"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0',
    'accept': 'application/json, text/plain, */*',
    'sec-ch-ua': '"Microsoft Edge";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'content-type': 'application/json',
    'sec-ch-ua-mobile': '?0',
    'origin': 'https://ted.europa.eu',
    'sec-fetch-site': 'same-site',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://ted.europa.eu/',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'cookie': 'GUEST_LANGUAGE_ID=en_GB; COOKIE_SUPPORT=true; cck1=%7B%22cm%22%3Atrue%2C%22all1st%22%3Atrue%2C%22closed%22%3Afalse%7D; route=1750647858.277.364.784926|726825d00aba56cccab96f4e82375684'
}

# 设置请求体
request_body = {
    "query": "publication-number=401988-2025",
    "page": 1,
    "limit": 50,
    "fields": [
        "family",
        "amended-by",
        "cancelled-by",
        "last-version",
        "extended-by",
        "notice-standard-version",
        "procedure-identifier",
        "main-classification-proc",
        "classification-cpv",
        "buyer-country",
        "buyer-profile",
        "translation-languages",
        "notice-type",
        "notice-title",
        "latest-version"
    ],
    "validation": False,
    "scope": "ALL",
    "language": "EN",
    "onlyLatestVersions": False,
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

def extract_and_save_data(json_data, publication_number):
    """从JSON数据中提取有用信息并保存为CSV"""
    try:
        # 检查是否有通知数据
        if not json_data.get("notices"):
            logger.warning("No notice information found in JSON data")
            return

        # 获取第一个通知（在这个例子中只有一个）
        notice = json_data["notices"][0]

        # 提取有用信息
        extracted_data = {
            "publication_number": notice.get("publication-number", ""),
            "notice_type": notice.get("notice-type", {}).get("label", ""),
            "procedure_id": notice.get("procedure-identifier", ""),
            "country": ", ".join([country.get("label", "") for country in notice.get("buyer-country", [])]),
            "main_cpv": ", ".join([cpv.get("label", "") for cpv in notice.get("main-classification-proc", [])]),
            "cpv_codes": ", ".join(
                [f"{cpv.get('value', '')}: {cpv.get('label', '')}" for cpv in notice.get("classification-cpv", [])]),
            "buyer_profile": ", ".join(notice.get("buyer-profile", [])),
            "title_en": notice.get("notice-title", {}).get("eng", ""),
            "title_es": notice.get("notice-title", {}).get("spa", ""),
            "pdf_url_en": notice.get("links", {}).get("pdf", {}).get("ENG", ""),
            "html_url_en": notice.get("links", {}).get("html", {}).get("ENG", ""),
        }

        # 提取facets中的额外信息
        facets = json_data.get("facets", {})
        if facets:
            # 提取程序类型
            procedure_types = facets.get("procedure-type", [])
            if procedure_types:
                extracted_data["procedure_type"] = procedure_types[0].get("label", "")

            # 提取合同性质
            contract_natures = facets.get("contract-nature", [])
            if contract_natures:
                extracted_data["contract_nature"] = contract_natures[0].get("label", "")

            # 提取出版年份
            pub_dates = facets.get("publication-date", [])
            if pub_dates:
                extracted_data["publication_year"] = pub_dates[0].get("label", "")

        # 将提取的数据转换为DataFrame
        df = pd.DataFrame([extracted_data])

        # 保存为CSV
        csv_filename = f"data/tender_{publication_number}.csv"
        df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        logger.info(f"Extracted data saved to: {csv_filename}")

        return extracted_data

    except Exception as e:
        logger.error(f"Error extracting and saving data: {e}")
        return None


# 如果已经有缓存的JSON文件，可以直接从中提取数据
def process_cached_json(json_file_path):
    """处理已缓存的JSON文件并提取数据"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        # 从文件名中提取出版号
        filename = os.path.basename(json_file_path)
        parts = filename.split('_')
        if len(parts) >= 3:
            publication_number = parts[2].split('.')[0]
        else:
            publication_number = "unknown"

        # 提取并保存数据
        return extract_and_save_data(json_data, publication_number)

    except Exception as e:
        logger.error(f"Error processing cached JSON file: {e}")
        return None


# 确保cache目录存在
os.makedirs('cache', exist_ok=True)
# 确保data目录存在
os.makedirs('data', exist_ok=True)

# 从请求体中提取出版号和页码用于文件命名
publication_number = request_body["query"].split("=")[1] if "=" in request_body["query"] else "unknown"
page_number = request_body["page"]

try:
    # 发送 POST 请求，获取响应
    response = requests.post(url, headers=headers, json=request_body)
    
    # 检查请求是否成功
    if response.status_code == 200:
        logger.info("Request successful!")
        
        # 记录响应头信息，帮助调试
        logger.debug(f"Response headers: {response.headers}")
        
        try:
            # 尝试直接获取JSON数据（requests应该会自动处理解压）
            json_data = response.json()
            logger.info("Successfully parsed JSON data")
            
            # 将JSON数据保存到文件
            json_filename = f"cache/page_{page_number}_{publication_number}.json"
            with open(json_filename, 'w', encoding='utf-8') as json_file:
                json.dump(json_data, json_file, ensure_ascii=False, indent=2)
            
            logger.info(f"JSON data saved to file: {json_filename}")
            
            # 提取有用信息
            extract_and_save_data(json_data, publication_number)
            
        except Exception as e:
            logger.error(f"Failed to parse JSON: {e}")
            
            # 尝试获取文本内容
            try:
                text_content = response.text
                logger.info(f"Response text content length: {len(text_content)} characters")
                logger.debug(f"First 100 characters of response: {text_content[:100]}")
                
                # 尝试手动解析JSON
                try:
                    json_data = json.loads(text_content)
                    logger.info("Manual JSON parsing successful")
                    
                    # 将JSON数据保存到文件
                    json_filename = f"cache/page_{page_number}_{publication_number}.json"
                    with open(json_filename, 'w', encoding='utf-8') as json_file:
                        json.dump(json_data, json_file, ensure_ascii=False, indent=2)
                    
                    logger.info(f"JSON data saved to file: {json_filename}")
                    
                    # 提取有用信息
                    extract_and_save_data(json_data, publication_number)
                    
                except json.JSONDecodeError as je:
                    logger.error(f"Manual JSON parsing failed: {je}")
                    
                    # 保存文本内容到文件
                    text_filename = f"cache/response_text_{publication_number}-{page_number}.txt"
                    with open(text_filename, 'w', encoding='utf-8') as f:
                        f.write(text_content)
                    logger.info(f"Response text content saved to: {text_filename}")
                    
            except Exception as te:
                logger.error(f"Failed to get text content: {te}")
                
            # 保存原始响应内容到文件
            raw_filename = f"cache/raw_response_{publication_number}-{page_number}.bin"
            with open(raw_filename, 'wb') as f:
                f.write(response.content)
            logger.info(f"Raw response content saved to: {raw_filename}")
            
    else:
        logger.error(f"Request failed with status code: {response.status_code}")
        # 保存错误响应到文件
        error_filename = f"cache/error_response_{publication_number}-{page_number}.txt"
        with open(error_filename, 'wb') as f:
            f.write(response.content)
        logger.info(f"Error response saved to: {error_filename}")
        
except Exception as req_error:
    logger.error(f"Error during request: {req_error}")


# 如果脚本直接运行，处理指定的缓存文件
if __name__ == "__main__":
    # 可以通过命令行参数指定要处理的缓存文件
    import sys

    if len(sys.argv) > 1:
        cache_file = sys.argv[1]
        if os.path.exists(cache_file):
            logger.info(f"Processing cache file: {cache_file}")
            process_cached_json(cache_file)
        else:
            logger.error(f"Cache file does not exist: {cache_file}")
    else:
        # 如果没有指定缓存文件，可以处理cache目录下的所有JSON文件
        cache_dir = "cache"
        if os.path.exists(cache_dir):
            json_files = [f for f in os.listdir(cache_dir) if f.endswith('.json')]
            if json_files:
                for json_file in json_files:
                    logger.info(f"Processing cache file: {json_file}")
                    process_cached_json(os.path.join(cache_dir, json_file))
            else:
                logger.warning(f"No JSON files found in {cache_dir} directory")
