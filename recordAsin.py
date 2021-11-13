import json

import jsonlines
import pandas as pd

df = pd.read_csv('newAsin.csv')  # 存储是否被爬取的记录
movies = open("movie.json", encoding='UTF-8')

for item in jsonlines.Reader(movies):
    asin = item['ASIN']
    # print(df.loc[df['asin'] == asin])
    df.loc[df['asin'] == asin, 'isGot'] = 1

df.to_csv("newAsin.csv", index=False)