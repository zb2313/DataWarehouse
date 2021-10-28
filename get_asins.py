import json
import pandas as pd

f = open("E:\\Movies_and_TV_5\\Movies_and_TV_5.json")
js = []
formerAsin = "null"
for line in f.readlines():
    js_l = json.loads(line)

    # 简单去重
    Asin = js_l['asin']
    if Asin == formerAsin:
        continue
    formerAsin = Asin

    js.append(js_l['asin'])
    print(js_l['asin'])
test = pd.DataFrame(columns=['asin'], data=js).drop_duplicates()
test.to_csv('asin.csv')
# print(js)
