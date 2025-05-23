from DrissionPage import ChromiumPage
import pandas as pd
import time
import os

os.makedirs('data', exist_ok=True)
page = ChromiumPage()
url = "https://ted.europa.eu/en/search/result?classification-cpv=44000000%2C45000000&search-scope=ALL"
page.get(url)
time.sleep(3)

results = []

for i in range(10):
    print(f"抓取第{i+1}页")
    time.sleep(2)
    trs = page.eles('css=tbody tr')
    for tr in trs:
        tds = tr.eles('tag:td')
        if len(tds) < 4:
            continue
        title = tds[1].text
        link = tds[1].ele('tag:a').link if tds[1].ele('tag:a') else ''
        date = tds[0].text
        cpv = tds[2].text
        country = tds[3].text
    next_btn = page.ele('xpath=//button[.//span[text()=">"]]')
    if next_btn and not next_btn.disabled:
        next_btn.click()
    else:
        break

page.close()

df = pd.DataFrame(results, columns=["Title", "Link", "Date", "CPV", "Country"])
df.to_csv("data/ted_tenders_drissionpage.csv", index=False, encoding="utf-8")
print("抓取完成") 