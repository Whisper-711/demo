import os
import json
import time
import logging
import random
import re
from datetime import datetime

# 配置日志
log_filename = f"dicks_firefox_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_filename, encoding='utf-8')
    ]
)
logger = logging.getLogger('dicks_firefox')
logger.info('Starting Dick\'s Sporting Goods Firefox crawler')

# 创建缓存目录
cache_dir = 'cache'
os.makedirs(cache_dir, exist_ok=True)

# 提示安装DrissionPage
logger.info("请确保已安装DrissionPage库：pip install DrissionPage")

try:
    from DrissionPage import ChromiumPage, ChromiumOptions, FirefoxPage, FirefoxOptions
    from DrissionPage import SessionPage
    from DrissionPage.common import By
except ImportError:
    logger.error("请先安装DrissionPage库：pip install DrissionPage")
    exit(1)

def random_sleep(min_seconds=1, max_seconds=3):
    """随机等待一段时间，模拟人类行为"""
    sleep_time = random.uniform(min_seconds, max_seconds)
    logger.info(f"随机等待 {sleep_time:.2f} 秒...")
    time.sleep(sleep_time)

def setup_firefox():
    """设置Firefox浏览器，添加反爬虫措施"""
    logger.info("设置Firefox浏览器...")
    
    # 创建FirefoxOptions对象并设置选项
    options = FirefoxOptions()
    options.set_browser_path(r'C:\Program Files\Mozilla Firefox\firefox.exe')  # 根据实际Firefox路径调整
    options.headless = False  # 设置为可见模式，以便观察
    
    # 设置用户代理为Firefox
    options.set_user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0')
    
    # 设置窗口大小
    options.set_argument('--window-size=1920,1080')
    
    # 创建FirefoxPage对象
    page = FirefoxPage(options)
    
    return page

def extract_category_id(url):
    """从URL中提取分类ID"""
    # 提取 /c/category-name 或 /f/category-name 中的ID
    c_pattern = r'/c/([a-zA-Z0-9\-_]+)'
    f_pattern = r'/f/([a-zA-Z0-9\-_]+)'
    
    c_match = re.search(c_pattern, url)
    if c_match:
        return c_match.group(1)
    
    f_match = re.search(f_pattern, url)
    if f_match:
        return f_match.group(1)
    
    return None

def crawl_categories():
    """爬取Dick's Sporting Goods商品分类"""
    logger.info("开始爬取商品分类...")
    
    # 设置Firefox浏览器
    page = setup_firefox()
    
    try:
        # 访问主页
        logger.info("访问主页...")
        page.get('https://www.dickssportinggoods.com/')
        
        # 等待页面加载
        logger.info("等待页面加载...")
        try:
            page.wait.doc_loaded()
        except Exception as e:
            logger.warning(f"等待页面加载时出错: {str(e)}")
        
        # 随机等待，模拟人类行为
        random_sleep(3, 5)
        
        # 保存页面源代码
        page_source = page.html
        home_html_file = os.path.join(cache_dir, "dicks_homepage_firefox.html")
        with open(home_html_file, 'w', encoding='utf-8') as f:
            f.write(page_source)
        logger.info(f"首页源代码保存至: {home_html_file}")
        
        # 保存截图
        screenshot_file = os.path.join(cache_dir, "dicks_homepage_firefox.png")
        page.get_screenshot(screenshot_file)
        logger.info(f"首页截图保存至: {screenshot_file}")
        
        # 提取导航菜单
        logger.info("提取导航菜单...")
        
        # 尝试点击菜单按钮（如果有的话）
        try:
            menu_buttons = page.eles('xpath://button[contains(@class, "navigation") or contains(@class, "menu")]')
            if menu_buttons:
                menu_buttons[0].click()
                logger.info("已点击菜单按钮")
                random_sleep(1, 2)
        except Exception as e:
            logger.warning(f"点击菜单按钮时出错: {str(e)}")
        
        # 提取所有分类链接
        logger.info("提取所有分类链接...")
        all_links = page.run_js('''
            var links = [];
            var allLinks = document.querySelectorAll('a');
            
            for (var i = 0; i < allLinks.length; i++) {
                var href = allLinks[i].getAttribute('href');
                var text = allLinks[i].innerText.trim();
                
                if (href && text) {
                    links.push({
                        text: text,
                        href: href
                    });
                }
            }
            
            return JSON.stringify(links);
        ''')
        
        if all_links:
            try:
                links_data = json.loads(all_links)
                
                # 筛选分类链接
                category_links = []
                for link in links_data:
                    href = link.get('href', '')
                    text = link.get('text', '')
                    
                    # 判断是否是分类链接
                    if href and text and ('/c/' in href or '/f/' in href):
                        # 确保链接是完整URL
                        if not href.startswith('http'):
                            if href.startswith('/'):
                                href = f"https://www.dickssportinggoods.com{href}"
                            else:
                                href = f"https://www.dickssportinggoods.com/{href}"
                        
                        # 提取分类ID
                        category_id = extract_category_id(href)
                        
                        category_links.append({
                            'name': text.strip(),
                            'url': href,
                            'category_id': category_id
                        })
                
                # 保存分类链接
                if category_links:
                    links_file = os.path.join(cache_dir, "dicks_category_links_firefox.json")
                    with open(links_file, 'w', encoding='utf-8') as f:
                        json.dump(category_links, f, ensure_ascii=False, indent=2)
                    logger.info(f"找到 {len(category_links)} 个分类链接，保存至: {links_file}")
                else:
                    logger.warning("未找到分类链接")
            except json.JSONDecodeError:
                logger.error("解析链接数据失败")
        
        # 提取顶级分类和子分类
        logger.info("提取顶级分类和子分类结构...")
        
        # 查找顶级分类元素
        top_categories = []
        
        # 尝试不同的选择器来找到顶级分类
        selectors = [
            'xpath://nav//li[contains(@class, "category") or contains(@class, "menu-item")]',
            'css:nav .category, nav .menu-item',
            'xpath://div[contains(@class, "navigation") or contains(@class, "menu")]//li'
        ]
        
        for selector in selectors:
            try:
                category_elements = page.eles(selector)
                if category_elements:
                    logger.info(f"使用选择器 '{selector}' 找到 {len(category_elements)} 个顶级分类元素")
                    break
            except Exception as e:
                logger.warning(f"使用选择器 '{selector}' 查找顶级分类时出错: {str(e)}")
        
        # 提取顶级分类和子分类的结构
        category_structure = []
        
        # 使用JavaScript提取分类结构
        structure_data = page.run_js('''
            function extractCategories(element) {
                var categories = [];
                var menuItems = element.querySelectorAll('.menu-item, .category, li');
                
                for (var i = 0; i < menuItems.length; i++) {
                    var item = menuItems[i];
                    var linkElement = item.querySelector('a');
                    
                    if (!linkElement) continue;
                    
                    var href = linkElement.getAttribute('href');
                    var text = linkElement.innerText.trim();
                    
                    if (!href || !text) continue;
                    
                    var category = {
                        name: text,
                        url: href
                    };
                    
                    // 查找子分类
                    var subMenu = item.querySelector('.sub-menu, .dropdown, ul');
                    if (subMenu) {
                        var subCategories = [];
                        var subLinks = subMenu.querySelectorAll('a');
                        
                        for (var j = 0; j < subLinks.length; j++) {
                            var subHref = subLinks[j].getAttribute('href');
                            var subText = subLinks[j].innerText.trim();
                            
                            if (subHref && subText) {
                                subCategories.push({
                                    name: subText,
                                    url: subHref
                                });
                            }
                        }
                        
                        if (subCategories.length > 0) {
                            category.subcategories = subCategories;
                        }
                    }
                    
                    categories.push(category);
                }
                
                return categories;
            }
            
            var navElements = document.querySelectorAll('nav, .navigation, .menu');
            var allCategories = [];
            
            for (var i = 0; i < navElements.length; i++) {
                var categories = extractCategories(navElements[i]);
                if (categories.length > 0) {
                    allCategories = allCategories.concat(categories);
                }
            }
            
            return JSON.stringify(allCategories);
        ''')
        
        if structure_data:
            try:
                category_structure = json.loads(structure_data)
                
                # 处理分类结构数据
                processed_structure = []
                
                for category in category_structure:
                    name = category.get('name', '')
                    url = category.get('url', '')
                    
                    if name and url and ('/c/' in url or '/f/' in url):
                        # 确保链接是完整URL
                        if not url.startswith('http'):
                            if url.startswith('/'):
                                url = f"https://www.dickssportinggoods.com{url}"
                            else:
                                url = f"https://www.dickssportinggoods.com/{url}"
                        
                        # 提取分类ID
                        category_id = extract_category_id(url)
                        
                        processed_category = {
                            'name': name.strip(),
                            'url': url,
                            'category_id': category_id
                        }
                        
                        # 处理子分类
                        subcategories = category.get('subcategories', [])
                        if subcategories:
                            processed_subcategories = []
                            
                            for subcategory in subcategories:
                                sub_name = subcategory.get('name', '')
                                sub_url = subcategory.get('url', '')
                                
                                if sub_name and sub_url and ('/c/' in sub_url or '/f/' in sub_url):
                                    # 确保链接是完整URL
                                    if not sub_url.startswith('http'):
                                        if sub_url.startswith('/'):
                                            sub_url = f"https://www.dickssportinggoods.com{sub_url}"
                                        else:
                                            sub_url = f"https://www.dickssportinggoods.com/{sub_url}"
                                    
                                    # 提取分类ID
                                    sub_category_id = extract_category_id(sub_url)
                                    
                                    processed_subcategories.append({
                                        'name': sub_name.strip(),
                                        'url': sub_url,
                                        'category_id': sub_category_id
                                    })
                            
                            if processed_subcategories:
                                processed_category['subcategories'] = processed_subcategories
                        
                        processed_structure.append(processed_category)
                
                # 保存分类结构
                if processed_structure:
                    structure_file = os.path.join(cache_dir, "dicks_category_structure_firefox.json")
                    with open(structure_file, 'w', encoding='utf-8') as f:
                        json.dump(processed_structure, f, ensure_ascii=False, indent=2)
                    logger.info(f"提取了 {len(processed_structure)} 个顶级分类的结构，保存至: {structure_file}")
            except json.JSONDecodeError:
                logger.error("解析分类结构数据失败")
        
        # 提取全局状态数据
        logger.info("提取全局状态数据...")
        global_state = page.run_js('''
            var data = {};
            
            // 尝试获取各种全局状态
            if (window.__INITIAL_STATE__) {
                data.initialState = window.__INITIAL_STATE__;
            }
            if (window.__NAVIGATION_STATE__) {
                data.navigationState = window.__NAVIGATION_STATE__;
            }
            if (window.__CATEGORY_STATE__) {
                data.categoryState = window.__CATEGORY_STATE__;
            }
            
            return JSON.stringify(data);
        ''')
        
        if global_state:
            try:
                state_data = json.loads(global_state)
                state_file = os.path.join(cache_dir, "dicks_global_state_firefox.json")
                with open(state_file, 'w', encoding='utf-8') as f:
                    json.dump(state_data, f, ensure_ascii=False, indent=2)
                logger.info(f"全局状态数据保存至: {state_file}")
                
                # 从全局状态中提取分类信息
                if 'navigationState' in state_data:
                    nav_state = state_data['navigationState']
                    if 'categories' in nav_state:
                        nav_categories = nav_state['categories']
                        nav_categories_file = os.path.join(cache_dir, "dicks_navigation_categories.json")
                        with open(nav_categories_file, 'w', encoding='utf-8') as f:
                            json.dump(nav_categories, f, ensure_ascii=False, indent=2)
                        logger.info(f"从全局状态中提取的导航分类保存至: {nav_categories_file}")
            except json.JSONDecodeError:
                logger.error("解析全局状态数据失败")
        
        # 尝试使用网络请求获取分类API数据
        logger.info("尝试获取分类API数据...")
        
        # 捕获网络请求
        network_data = page.run_js('''
            var requests = [];
            
            if (window.performance && window.performance.getEntries) {
                var entries = window.performance.getEntries();
                
                for (var i = 0; i < entries.length; i++) {
                    var entry = entries[i];
                    
                    if (entry.entryType === 'resource' && 
                        (entry.name.includes('/api/') || 
                         entry.name.includes('/category/') || 
                         entry.name.includes('/categories/'))) {
                        
                        requests.push({
                            url: entry.name,
                            initiatorType: entry.initiatorType,
                            duration: entry.duration
                        });
                    }
                }
            }
            
            return JSON.stringify(requests);
        ''')
        
        if network_data:
            try:
                api_data = json.loads(network_data)
                api_file = os.path.join(cache_dir, "dicks_api_requests_firefox.json")
                with open(api_file, 'w', encoding='utf-8') as f:
                    json.dump(api_data, f, ensure_ascii=False, indent=2)
                logger.info(f"API请求数据保存至: {api_file}")
                
                # 尝试请求分类API
                category_apis = [req['url'] for req in api_data if 'category' in req['url'].lower()]
                
                if category_apis:
                    logger.info(f"找到 {len(category_apis)} 个可能的分类API")
                    
                    # 创建SessionPage对象用于发送请求
                    session = SessionPage()
                    session.set_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0')
                    session.set_header('Referer', 'https://www.dickssportinggoods.com/')
                    
                    # 尝试请求每个API
                    for idx, api_url in enumerate(category_apis[:3]):  # 限制只请求前3个API
                        try:
                            logger.info(f"请求API [{idx+1}/{len(category_apis[:3])}]: {api_url}")
                            response = session.get(api_url)
                            
                            if response.status_code == 200:
                                try:
                                    api_response = response.json()
                                    api_response_file = os.path.join(cache_dir, f"dicks_api_response_{idx+1}.json")
                                    with open(api_response_file, 'w', encoding='utf-8') as f:
                                        json.dump(api_response, f, ensure_ascii=False, indent=2)
                                    logger.info(f"API响应数据保存至: {api_response_file}")
                                except:
                                    logger.warning(f"API响应不是有效的JSON")
                            else:
                                logger.warning(f"API请求失败: {response.status_code}")
                            
                            # 添加延迟
                            random_sleep(2, 3)
                        except Exception as e:
                            logger.error(f"请求API时出错: {str(e)}")
            except json.JSONDecodeError:
                logger.error("解析API请求数据失败")
        
        return True
        
    except Exception as e:
        logger.error(f"爬取过程中出错: {str(e)}")
        return False
    finally:
        # 关闭浏览器
        logger.info("关闭浏览器...")
        try:
            page.quit()
        except Exception as e:
            logger.error(f"关闭浏览器时出错: {str(e)}")

if __name__ == "__main__":
    logger.info("开始爬取Dick's Sporting Goods分类数据")
    
    # 爬取数据
    result = crawl_categories()
    
    if result:
        logger.info("数据爬取成功")
        logger.info("所有分类数据已保存到cache目录")
        logger.info("您可以使用这些分类ID来爬取具体产品")
    else:
        logger.error("数据爬取失败")
