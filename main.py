# 作者：韩乃超
# 日期：2021年10月29日


import csv
import time
from concurrent.futures import ThreadPoolExecutor

import eventlet
import pandas as pd
import requests  # 导入requests包
from bs4 import BeautifulSoup

sleep_time = 1
long_sleep_time = 60 * 2
tol_attempts = 0  # 某段时间总尝试次数
success_attempts = 0  # 该段时间内的成功次数
cookies = {}
df = pd.read_csv('newAsin.csv')  # 存储是否被爬取的记录

web_header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36',
    "Connection": "closer",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
    "Referer": "http: // www.google.com",
    'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'accept-encoding': 'gzip, deflate, br'

}


def getStrHtml(url):
    try:
        global tol_attempts  # 总的尝试次数
        tol_attempts += 1

        strhtml = requests.get(url, headers=web_header, cookies=cookies)
        # print(strhtml.status_code)
        soup = BeautifulSoup(strhtml.text, 'lxml')
        movie_title = str(soup.select('title')[0].getText())

        if movie_title == "Amazon.com" or movie_title == "Sorry! Something went wrong!" or movie_title == 'Robot Check':
            print(movie_title)
            return -1
        print(movie_title)

        global success_attempts
        success_attempts += 1  # 成功的尝试次数

        data = soup.select('#imdbInfo_feature_div > span > i')
        emptyData = soup.select('#cannotbefound')  # 创造一个空的data

        if data != emptyData:  # 判断是不是电影
            print(url)
            print("movie")
            return soup
        else:
            print(url)
            print("not movie")
            return 0
    except Exception as e:
        print('[error]', e)
        return -1


def download_one_page(url, lineNum):
    # print(lineNum)

    # 如果已经被写入，则直接跳过
    if df.loc[lineNum, 'isGot'] == 1:
        return

    soup = getStrHtml(url)  # 尝试一次
    attempts = 0

    while soup == -1:  # 不成功继续尝试
        attempts += 1
        time.sleep(sleep_time)
        with eventlet.Timeout(10, False):
            soup = getStrHtml(url)

    # 爬取成功后记录已爬取信息
    df.loc[lineNum, 'isGot'] = 1
    df.to_csv("newAsin.csv", index=False)

    # 在这里存储爬取的信息
    dictionary = {
        'movie_id': df.loc[lineNum, 'asin'],
        # 'actors': soup.get('xxxxx'),
        # 'director': soup.get('xxxxx'),
        # .......
    }
    # 待完成
    # 待完成
    # 待完成
    # 

    global tol_attempts, success_attempts
    print(tol_attempts, success_attempts, 'suc_rate:', success_attempts / tol_attempts)
    if success_attempts / tol_attempts < 0.4:  # 成功率低于0.2的话休息2分钟
        time.sleep(long_sleep_time)
        success_attempts = 0  # 重置计数
        tol_attempts = 0
    if tol_attempts > 100:  # 每一百次尝试重置一次计数
        success_attempts = 0
        tol_attempts = 0


if __name__ == '__main__':

    asinFile = open("asin.csv")
    reader = csv.reader(asinFile)
    # 创建线程池
    with ThreadPoolExecutor(20) as t:
        for item in reader:
            if reader.line_num == 1:
                continue
            url = 'https://www.amazon.com/dp/' + item[1]
            t.submit(download_one_page, url, reader.line_num)

    asinFile.close()

    print("全部下载完毕!")
