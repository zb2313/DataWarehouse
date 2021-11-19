import jsonlines
import pandas as pd

df = pd.read_csv('newAsin.csv')  # 存储是否被爬取的记录
movies = open("movie.json", encoding='UTF-8')

count = 0
notMoviecount = 0
for item in jsonlines.Reader(movies):
    asin = item['ASIN']

    if 'isMovie' in item:
        notMoviecount += 1
    # print(df.loc[df['asin'] == asin])
    df.loc[df['asin'] == asin, 'isGot'] = 1

    count += 1

df.to_csv("newAsin.csv", index=False)
print(count)
print(notMoviecount)
