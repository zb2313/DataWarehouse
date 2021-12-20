import json

import pandas as pd

def extractAsin():
    f = open("../../data/Movies_and_TV_5.json")
    js = []
    isGot = []
    formerAsin = "null"
    for line in f.readlines():
        js_l = json.loads(line)
        # 简单去重
        Asin = js_l['asin']
        if Asin == formerAsin:
            continue
        formerAsin = Asin

        js.append(js_l['asin'])
        isGot.append(0)
        print(js_l['asin'])

    dictionary = {'asin': js, "isGot": isGot}
    # test = pd.DataFrame(columns=['asin'], data=js).drop_duplicates()
    test = pd.DataFrame(dictionary).drop_duplicates()
    test.to_csv('../../data/newAsin.csv', index=False) # 提取出的asin