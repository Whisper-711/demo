import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from urllib3.exceptions import InsecureRequestWarning

#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class ATISurchargeScraper:
    def __init__(self):
        self.url = "https://www.atimaterials.com/specialtyrolledproducts/Pages/surcharge-history.aspx"

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        self.alloy_types = [
            "Steel",
            "HTANickel",
            "Tool",
            "Titanium"
        ]

        self.years = ["2025", "2024", "2023", "2022", "2021", "2020", "2019"]

        self.output_dir = "ati_surcharge_data"
        os.makedirs(self.output_dir, exist_ok=True)

    def extract_hidden_fields(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        hidden_fields = {}

        for hidden_input in soup.find_all('input', type='hidden'):
            if hidden_input.get('name'):
                hidden_fields[hidden_input.get('name')] = hidden_input.get('value', '')

        return hidden_fields

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
        try:
            print("开始爬取ATI材料附加费数据...")

            print("正在获取初始页面...")
            response = requests.get(self.url, headers=self.headers, verify=False)
            response.raise_for_status()

            hidden_fields = self.extract_hidden_fields(response.text)

            all_data = {}

            for alloy_type in self.alloy_types:
                print(f"\n处理合金类型: {alloy_type}")
                all_data[alloy_type] = {}

                print(f"选择合金类型: {alloy_type}")
                form_data = self.create_form_data(hidden_fields, alloy_type)

                response = requests.post(self.url, headers=self.headers, data=form_data, verify=False)
                response.raise_for_status()

                hidden_fields = self.extract_hidden_fields(response.text)

                for year in self.years:
                    print(f"  处理年份: {year}")

                    form_data = self.create_form_data(hidden_fields, alloy_type, year)

                    response = requests.post(self.url, headers=self.headers, data=form_data, verify=False)
                    response.raise_for_status()

                    hidden_fields = self.extract_hidden_fields(response.text)

                    headers, rows = self.extract_table_data(response.text)

                    df = self.process_table_data(headers, rows, alloy_type, year)
                    if df is not None:
                        all_data[alloy_type][year] = df


                if all_data[alloy_type]:
                    dfs = list(all_data[alloy_type].values())
                    if dfs:
                        combined_df = pd.concat(dfs, keys=list(all_data[alloy_type].keys()), names=['Year', 'Row'])
                        combined_file = os.path.join(self.output_dir, f"{alloy_type}_surcharge_history.xlsx")
                        combined_df.to_excel(combined_file)
                        print(f"已合并保存 {alloy_type} 的所有年份数据到 {combined_file}")

            all_dfs = []
            for alloy_dfs in all_data.values():
                all_dfs.extend(alloy_dfs.values())

            if all_dfs:
                master_df = pd.concat(all_dfs, ignore_index=True)
                master_file = os.path.join(self.output_dir, "all_surcharge_history.xlsx")
                master_df.to_excel(master_file, index=False)
                print(f"\n已合并所有数据到 {master_file}")

            print("\n爬取完成！")
            return all_data

        except Exception as e:
            print(f"爬取过程中发生错误: {e}")
            import traceback
            traceback.print_exc()
            return None


if __name__ == "__main__":
    scraper = ATISurchargeScraper()
    scraper.scrape()