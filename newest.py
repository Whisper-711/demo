import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from urllib3.exceptions import InsecureRequestWarning
import datetime

# 禁用SSL警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class ATISurchargeScraper:
    def __init__(self):
        # 设置目标URL
        self.url = "https://www.atimaterials.com/specialtyrolledproducts/Pages/surcharge-history.aspx"

        # 设置请求头
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        # 请求相关设置
        self.timeout = 30  # 请求超时时间(秒)
        self.max_retries = 3  # 最大重试次数
        self.retry_delay = 3  # 重试间隔(秒)

        # 合金类型选项
        self.alloy_types = [
            "Steel",  # 不锈钢
            "HTANickel",  # 镍钴合金
            "Tool",  # 装甲
            "Titanium"  # 钛
        ]

        self.years = ["2025", "2024", "2023", "2022", "2021", "2020", "2019"]

        self.output_dir = "ati_surcharge_data"
        os.makedirs(self.output_dir, exist_ok=True)

    def extract_hidden_fields(self, html_content):
        """提取所有隐藏字段"""
        soup = BeautifulSoup(html_content, 'html.parser')
        hidden_fields = {}

        # 查找所有隐藏输入字段
        for hidden_input in soup.find_all('input', type='hidden'):
            if hidden_input.get('name'):
                hidden_fields[hidden_input.get('name')] = hidden_input.get('value', '')

        return hidden_fields

    def make_request_with_retry(self, method, url, **kwargs):
        """发送请求并自动重试"""
        retries = 0
        while retries <= self.max_retries:
            try:
                if method.lower() == 'get':
                    response = requests.get(url, timeout=self.timeout, **kwargs)
                else:
                    response = requests.post(url, timeout=self.timeout, **kwargs)
                response.raise_for_status()
                return response
            except (requests.RequestException, requests.ConnectionError, requests.Timeout) as e:
                retries += 1
                if retries > self.max_retries:
                    raise
                print(f"请求失败，{self.retry_delay}秒后重试 ({retries}/{self.max_retries})... 错误: {e}")
                time.sleep(self.retry_delay)

    def create_form_data(self, hidden_fields, alloy_type, year=None):
        form_data = {
            "__EVENTTARGET": "ctl00$ctl39$g_13235907_38e2_4756_80cf_f4e5f1573dee$ctl00$alloyType",
            "ctl00$ctl39$g_13235907_38e2_4756_80cf_f4e5f1573dee$ctl00$alloyType": alloy_type
        }

        if year:
            form_data["ctl00$ctl39$g_13235907_38e2_4756_80cf_f4e5f1573dee$ctl00$yearsAvailable"] = year

        form_data.update(hidden_fields)

        return form_data

    def extract_table_data(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')

        table = soup.select_one('table.ms-rteTable-default')
        if not table:
            table = soup.select_one('table')

        if not table:
            print("未找到表格")
            return [], []

        header_row = table.select_one('thead tr') or table.select_one('tr')
        if not header_row:
            print("未找到表头行")
            return [], []

        headers = [th.get_text(strip=True) for th in header_row.select('th')]
        if not headers:
            headers = [td.get_text(strip=True) for td in header_row.select('td')]

        rows = []
        data_rows = table.select('tbody tr') if table.select_one('tbody') else table.select('tr')[1:]

        for tr in data_rows:
            cells = [td.get_text(strip=True) for td in tr.select('td')]
            if cells:
                if len(cells) < len(headers):
                    cells.extend([''] * (len(headers) - len(cells)))
                elif len(cells) > len(headers):
                    cells = cells[:len(headers)]
                rows.append(cells)

        return headers, rows

    def process_table_data(self, headers, rows, alloy_type, year):
        if not rows:
            print(f"未找到 {alloy_type} {year} 的数据")
            return None

        df = pd.DataFrame(rows, columns=headers)

        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()

        for col in df.columns[1:]:
            try:
                df[col] = pd.to_numeric(df[col])
            except:
                pass

        df['Alloy_Type'] = alloy_type
        df['Year'] = year

        file_name = f"{alloy_type}_{year}_surcharge.xlsx"
        file_path = os.path.join(self.output_dir, file_name)
        df.to_excel(file_path, index=False)
        print(f"已保存 {alloy_type} {year} 数据到 {file_path}")

        return df

    def scrape(self):
        """执行爬取过程"""
        start_time = datetime.datetime.now()
        try:
            print("开始爬取ATI材料附加费数据...")
            print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

            # 获取初始页面
            print("正在获取初始页面...")
            response = self.make_request_with_retry('get', self.url, headers=self.headers, verify=False)

            # 提取初始隐藏字段
            hidden_fields = self.extract_hidden_fields(response.text)

            # 存储所有爬取的数据
            all_data = {}
            
            total_items = len(self.alloy_types) * len(self.years)
            completed_items = 0

            # 爬取每种合金类型和年份的数据
            for alloy_type_idx, alloy_type in enumerate(self.alloy_types):
                print(f"\n处理合金类型: {alloy_type} ({alloy_type_idx+1}/{len(self.alloy_types)})")
                all_data[alloy_type] = {}

                # 首先，选择合金类型
                print(f"选择合金类型: {alloy_type}")
                form_data = self.create_form_data(hidden_fields, alloy_type)

                # 发送POST请求
                response = self.make_request_with_retry('post', self.url, headers=self.headers, data=form_data, verify=False)

                # 更新隐藏字段，为后续请求做准备
                hidden_fields = self.extract_hidden_fields(response.text)

                # 然后，处理每个年份
                for year_idx, year in enumerate(self.years):
                    completed_items += 1
                    progress = completed_items / total_items * 100
                    elapsed = datetime.datetime.now() - start_time
                    estimated_total = elapsed.total_seconds() / completed_items * total_items if completed_items > 0 else 0
                    estimated_remaining = estimated_total - elapsed.total_seconds()
                    
                    try:
                        print(f"  处理年份: {year} ({year_idx+1}/{len(self.years)}) - 总进度: {progress:.1f}%")
                        if elapsed.total_seconds() > 10:  # 只在运行超过10秒后显示时间估计
                            print(f"  已用时间: {str(elapsed).split('.')[0]}, 预计剩余: {datetime.timedelta(seconds=int(estimated_remaining))}")

                        # 创建包含年份选择的表单数据
                        form_data = self.create_form_data(hidden_fields, alloy_type, year)

                        # 发送POST请求
                        response = self.make_request_with_retry('post', self.url, headers=self.headers, data=form_data, verify=False)

                        # 更新隐藏字段，为下一次请求做准备
                        hidden_fields = self.extract_hidden_fields(response.text)

                        # 提取表格数据
                        headers, rows = self.extract_table_data(response.text)

                        # 处理表格数据
                        df = self.process_table_data(headers, rows, alloy_type, year)
                        if df is not None:
                            all_data[alloy_type][year] = df

                        # 休息一下，避免请求太频繁
                        time.sleep(1)
                    except Exception as e:
                        print(f"  处理 {alloy_type} {year} 时出错: {e}")
                        continue

                # 处理完当前合金类型的所有年份后，合并数据
                if all_data[alloy_type]:
                    try:
                        dfs = list(all_data[alloy_type].values())
                        if dfs:  # 确保列表不为空
                            combined_df = pd.concat(dfs, keys=list(all_data[alloy_type].keys()), names=['Year', 'Row'])
                            combined_file = os.path.join(self.output_dir, f"{alloy_type}_surcharge_history.xlsx")
                            combined_df.to_excel(combined_file)
                            print(f"已合并保存 {alloy_type} 的所有年份数据到 {combined_file}")
                    except Exception as e:
                        print(f"合并 {alloy_type} 数据时出错: {e}")

            # 合并所有数据
            try:
                all_dfs = []
                for alloy_dfs in all_data.values():
                    all_dfs.extend(alloy_dfs.values())

                if all_dfs:
                    master_df = pd.concat(all_dfs, ignore_index=True)
                    master_file = os.path.join(self.output_dir, "all_surcharge_history.xlsx")
                    master_df.to_excel(master_file, index=False)
                    print(f"\n已合并所有数据到 {master_file}")
            except Exception as e:
                print(f"合并所有数据时出错: {e}")

            end_time = datetime.datetime.now()
            elapsed = end_time - start_time
            print("\n爬取完成！")
            print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"总运行时间: {str(elapsed).split('.')[0]}")
            
            # 统计结果
            total_scraped = sum(len(data) for data in all_data.values() if data)
            print(f"成功爬取数据: {total_scraped}/{total_items} 项")
            
            return all_data

        except KeyboardInterrupt:
            end_time = datetime.datetime.now()
            elapsed = end_time - start_time
            print("\n\n程序被用户中断！已爬取的数据将被保存。")
            print(f"运行时间: {str(elapsed).split('.')[0]}")
            return all_data
        except Exception as e:
            print(f"爬取过程中发生错误: {e}")
            import traceback
            traceback.print_exc()
            return None


if __name__ == "__main__":
    scraper = ATISurchargeScraper()
    scraper.scrape()