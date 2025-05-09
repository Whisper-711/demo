import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

with open('http_raw.txt', 'r', encoding='utf-8') as file:
    html_content = file.read()

soup = BeautifulSoup(html_content, 'html.parser')

table = soup.find('table', {'id': 'ctl00_ctl39_g_13235907_38e2_4756_80cf_f4e5f1573dee_ctl00_historyGrid'})

if not table:
    print("表格未找到，尝试查找任何表格")
    table = soup.find('table', {'class': 'table'})

if table:
    headers = []
    header_row = table.find('tr')
    if header_row:
        headers = [th.text.strip() for th in header_row.find_all('th')]

    rows = []
    data_rows = table.find_all('tr')[1:]  # 跳过表头行
    for row in data_rows:
        # 提取每行的单元格数据
        cells = row.find_all('td')
        row_data = [cell.text.strip() for cell in cells]
        rows.append(row_data)
    

    df = pd.DataFrame(rows, columns=headers)

    df.to_csv('surcharge_history.csv', index=False, encoding='utf-8')
    print("数据已成功保存到 surcharge_history.csv")
else:
    print("未找到任何表格")


    potential_tables = re.findall(r'<table.*?</table>', html_content, re.DOTALL)
    print(f"找到 {len(potential_tables)} 个潜在表格")

    for i, line in enumerate(html_content.split('\n')):
        if "table" in line.lower() and ("class" in line.lower() or "id" in line.lower()):
            print(f"行 {i+1}: {line}") 