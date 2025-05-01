from DrissionPage import ChromiumPage
import pandas as pd
import time

page = ChromiumPage()

url = "https://www.atimaterials.com/specialtyrolledproducts/Pages/surcharge-history.aspx"
page.get(url)
time.sleep(3)

select_ele = page.ele('tag:select')
if select_ele:
    print(f"找到下拉菜单: {select_ele.tag}")
    options_text = []
    for opt in select_ele.eles('tag:option'):
        options_text.append(opt.text.strip())
    print(f"可用选项: {options_text}")
else:
    print("找不到下拉菜单")
    exit(1)

for category in options_text:
    print(f"\n正在爬取：{category}")

    try:
        select_ele = page.ele('tag:select')
        if not select_ele:
            print("重新获取页面")
            page.get(url)
            time.sleep(3)
            select_ele = page.ele('tag:select')

        select_ele.select(category)
        print({category})
        time.sleep(3)

        table_ele = page.ele('css:table.table-striped')

        headers = []
        th_elements = table_ele.eles('tag:th')
        for th in th_elements:
            headers.append(th.text.strip())

        if not headers:
            print("未找到表头")
            continue


        rows = []
        for tr in table_ele.eles('tag:tr')[1:]:
            cells = tr.eles('tag:td')
            if cells:
                row = [td.text.strip() for td in cells]
                rows.append(row)

        if rows:
            print(f"找到 {len(rows)} 行数据")
            df = pd.DataFrame(rows, columns=headers)
            file_name = category.replace(" ", "_").replace("&", "and").replace("/", "_")
            df.to_excel(f"{file_name}_surcharge_history.xlsx", index=False)
            print(f"{category} 数据已保存为 {file_name}_surcharge_history.xlsx")
        else:
            print(f"{category} 没有数据行")

    except Exception as e:
        exit(1)

page.close()
