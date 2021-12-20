import json
import csv
import pandas as pd


def createImageCSV():
    file1 = open("../../data/movie.json", encoding='utf-8')
    header = ['ASIN', 'image']
    dicts = []
    for item in file1.readlines():
        dic = json.loads(item)
        dicts.append({'ASIN': dic['ASIN'], 'image': 'none'})
    print(dicts)
    with open('../../data/image.csv', 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=header)  #
        writer.writeheader()  # 写入列名
        writer.writerows(dicts)
    file1.close()
