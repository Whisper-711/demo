import os
import json
import time
import logging
from datetime import datetime
from DrissionPage import ChromiumPage, ChromiumOptions

# 配置日志
log_filename = f"dicks_drissionpage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_filename, encoding='utf-8')
    ]
)
logger = logging.getLogger('dicks_drissionpage')
logger.info('开始使用DrissionPage爬取Dick\'s Sporting Goods商品分类')

# 创建缓存目录
cache_dir = 'cache'
os.makedirs(cache_dir, exist_ok=True)

class DicksSportingGoodsCrawler:
    def __init__(self):
        """初始化爬虫"""
        logger.info("初始化DrissionPage")
        
        # 配置ChromiumOptions
        options = ChromiumOptions()
        options.set_argument('--disable-blink-features=AutomationControlled')  # 禁用自动化控制检测
        options.set_argument('--disable-infobars')  # 禁用信息栏
        options.set_argument('--start-maximized')  # 最大化窗口
        options.set_argument('--disable-extensions')  # 禁用扩展
        options.set_argument('--disable-gpu')  # 禁用GPU加速
        
        # 设置用户代理
        options.set_user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # 创建ChromiumPage实例
        self.page = ChromiumPage(options=options)
        
        # 设置超时时间
        self.page.set.timeouts(30, 30, 30)
        
    def crawl_categories(self):
        """爬取Dick's Sporting Goods的商品分类"""
        try:
            # 访问主页
            url = "https://www.dickssportinggoods.com"
            logger.info(f"访问网站: {url}")
            self.page.get(url)
            
            # 等待页面加载
            logger.info("等待页面加载...")
            time.sleep(5)
            
            # 保存页面截图
            screenshot_path = os.path.join(cache_dir, "dicks_homepage_screenshot.png")
            self.page.get_screenshot(screenshot_path)
            logger.info(f"页面截图保存至: {screenshot_path}")
            
            # 保存页面源代码
            html_path = os.path.join(cache_dir, "dicks_homepage.html")
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(self.page.html)
            logger.info(f"页面源代码保存至: {html_path}")
            
            # 提取导航菜单中的主要分类
            logger.info("提取主要商品分类...")
            categories = []
            
            # 尝试查找导航菜单
            nav_elements = self.page.eles('css:.dsg-header-nav-menu a')
            if nav_elements:
                logger.info(f"找到 {len(nav_elements)} 个导航链接")
                
                for element in nav_elements:
                    try:
                        category = {
                            'name': element.text,
                            'url': element.link,
                        }
                        if category['name'] and category['url']:
                            categories.append(category)
                    except Exception as e:
                        logger.error(f"提取导航元素时出错: {str(e)}")
            
            # 如果没有找到导航菜单，尝试其他选择器
            if not categories:
                logger.info("尝试其他选择器查找分类...")
                # 尝试查找顶部导航
                nav_elements = self.page.eles('css:.dsg-header-top-level-nav a')
                if nav_elements:
                    logger.info(f"找到 {len(nav_elements)} 个顶部导航链接")
                    
                    for element in nav_elements:
                        try:
                            category = {
                                'name': element.text,
                                'url': element.link,
                            }
                            if category['name'] and category['url']:
                                categories.append(category)
                        except Exception as e:
                            logger.error(f"提取顶部导航元素时出错: {str(e)}")
            
            # 如果仍然没有找到分类，尝试执行JavaScript获取
            if not categories:
                logger.info("尝试通过JavaScript获取分类...")
                # 执行JavaScript获取所有链接
                links = self.page.run_js('''
                    const links = Array.from(document.querySelectorAll('a')).filter(a => 
                        a.href.includes('/c/') || 
                        a.href.includes('/f/') || 
                        a.href.includes('/category/'));
                    return links.map(a => ({name: a.innerText.trim(), url: a.href}))
                        .filter(link => link.name && link.url);
                ''')
                
                if links:
                    logger.info(f"通过JavaScript找到 {len(links)} 个可能的分类链接")
                    categories = links
            
            # 保存分类数据
            if categories:
                logger.info(f"总共提取到 {len(categories)} 个分类")
                categories_file = os.path.join(cache_dir, "dicks_categories.json")
                with open(categories_file, 'w', encoding='utf-8') as f:
                    json.dump(categories, f, ensure_ascii=False, indent=2)
                logger.info(f"分类数据保存至: {categories_file}")
                
                # 提取子分类
                self.extract_subcategories(categories)
            else:
                logger.warning("未找到任何分类")
                
            return categories
                
        except Exception as e:
            logger.error(f"爬取分类时出错: {str(e)}")
            return []
        finally:
            # 关闭浏览器
            self.page.quit()
            logger.info("浏览器已关闭")
    
    def extract_subcategories(self, main_categories):
        """提取子分类"""
        logger.info("开始提取子分类...")
        all_categories = {}
        
        # 只处理前3个主分类，避免请求过多
        for i, category in enumerate(main_categories[:3]):
            try:
                category_name = category['name']
                category_url = category['url']
                
                if not category_url.startswith('http'):
                    category_url = f"https://www.dickssportinggoods.com{category_url}"
                
                logger.info(f"访问分类页面: {category_name} - {category_url}")
                self.page.get(category_url)
                time.sleep(3)  # 等待页面加载
                
                # 保存分类页面截图
                screenshot_path = os.path.join(cache_dir, f"dicks_category_{i+1}.png")
                self.page.get_screenshot(screenshot_path)
                
                # 提取子分类
                subcategories = []
                
                # 尝试查找子分类链接
                subcategory_elements = self.page.eles('css:.dsg-left-nav-menu a, .dsg-category-list a')
                
                if subcategory_elements:
                    logger.info(f"在 {category_name} 分类中找到 {len(subcategory_elements)} 个子分类链接")
                    
                    for element in subcategory_elements:
                        try:
                            subcategory = {
                                'name': element.text,
                                'url': element.link,
                            }
                            if subcategory['name'] and subcategory['url']:
                                subcategories.append(subcategory)
                        except Exception as e:
                            logger.error(f"提取子分类元素时出错: {str(e)}")
                
                # 如果没有找到子分类，尝试执行JavaScript获取
                if not subcategories:
                    logger.info(f"尝试通过JavaScript获取 {category_name} 的子分类...")
                    # 执行JavaScript获取所有链接
                    links = self.page.run_js('''
                        const links = Array.from(document.querySelectorAll('a')).filter(a => 
                            (a.href.includes('/c/') || a.href.includes('/f/') || a.href.includes('/category/')) &&
                            a.innerText.trim().length > 0);
                        return links.map(a => ({name: a.innerText.trim(), url: a.href}))
                            .filter(link => link.name && link.url);
                    ''')
                    
                    if links:
                        logger.info(f"通过JavaScript在 {category_name} 中找到 {len(links)} 个可能的子分类链接")
                        subcategories = links
                
                all_categories[category_name] = {
                    'main_category': category,
                    'subcategories': subcategories
                }
                
                # 保存当前分类的子分类数据
                subcategories_file = os.path.join(cache_dir, f"dicks_subcategories_{i+1}.json")
                with open(subcategories_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'category': category_name,
                        'url': category_url,
                        'subcategories': subcategories
                    }, f, ensure_ascii=False, indent=2)
                logger.info(f"{category_name} 的子分类数据保存至: {subcategories_file}")
                
            except Exception as e:
                logger.error(f"提取 {category.get('name', '未知')} 的子分类时出错: {str(e)}")
        
        # 保存所有分类数据
        all_categories_file = os.path.join(cache_dir, "dicks_all_categories.json")
        with open(all_categories_file, 'w', encoding='utf-8') as f:
            json.dump(all_categories, f, ensure_ascii=False, indent=2)
        logger.info(f"所有分类数据保存至: {all_categories_file}")
    
    def extract_api_urls(self):
        """提取API URL"""
        logger.info("尝试提取API URL...")
        
        try:
            # 访问男士鞋类页面
            url = "https://www.dickssportinggoods.com/f/all-mens-footwear"
            logger.info(f"访问页面: {url}")
            self.page.get(url)
            
            # 等待页面加载
            logger.info("等待页面加载...")
            time.sleep(5)
            
            # 保存页面截图
            screenshot_path = os.path.join(cache_dir, "dicks_mens_footwear_screenshot.png")
            self.page.get_screenshot(screenshot_path)
            logger.info(f"页面截图保存至: {screenshot_path}")
            
            # 获取网络请求
            logger.info("获取网络请求...")
            api_urls = self.page.run_js('''
                const performance = window.performance || window.mozPerformance || window.msPerformance || window.webkitPerformance || {};
                const entries = performance.getEntries ? performance.getEntries() : [];
                return entries
                    .filter(entry => entry.initiatorType === 'fetch' || entry.initiatorType === 'xmlhttprequest')
                    .filter(entry => entry.name.includes('api'))
                    .map(entry => entry.name);
            ''')
            
            if api_urls:
                logger.info(f"找到 {len(api_urls)} 个API URL")
                api_urls_file = os.path.join(cache_dir, "dicks_api_urls.json")
                with open(api_urls_file, 'w', encoding='utf-8') as f:
                    json.dump(api_urls, f, ensure_ascii=False, indent=2)
                logger.info(f"API URL保存至: {api_urls_file}")
            else:
                logger.warning("未找到任何API URL")
            
            # 尝试提取页面中的JSON数据
            logger.info("提取页面中的JSON数据...")
            json_data = self.page.run_js('''
                const data = {};
                if (window.__INITIAL_STATE__) data.initialState = window.__INITIAL_STATE__;
                if (window.__PRODUCT_LIST_STATE__) data.productListState = window.__PRODUCT_LIST_STATE__;
                if (window.__PRELOADED_STATE__) data.preloadedState = window.__PRELOADED_STATE__;
                return data;
            ''')
            
            if json_data:
                logger.info("成功提取页面中的JSON数据")
                json_data_file = os.path.join(cache_dir, "dicks_page_json_data.json")
                with open(json_data_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, ensure_ascii=False, indent=2)
                logger.info(f"页面JSON数据保存至: {json_data_file}")
            else:
                logger.warning("未找到页面中的JSON数据")
            
            return api_urls
            
        except Exception as e:
            logger.error(f"提取API URL时出错: {str(e)}")
            return []

if __name__ == "__main__":
    crawler = DicksSportingGoodsCrawler()
    
    # 爬取商品分类
    logger.info("开始爬取Dick's Sporting Goods商品分类")
    categories = crawler.crawl_categories()
    
    if categories:
        logger.info(f"成功爬取 {len(categories)} 个商品分类")
        logger.info("所有数据已保存到cache目录")
    else:
        logger.error("爬取商品分类失败")
