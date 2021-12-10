# 作者：韩乃超
# 日期：2021年10月29日
######
#
#
#   使用方法：先运行get_asins.py生成爬取记录表 newAsin.csv
#   接着运行本文件
#   运行后使用recordAsin.py记录下爬取的movie asin号（记录在newAsin.csv中）

import csv
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
import pandas
import eventlet

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import jsonlines
import pandas as pd
import requests  # 导入requests包
from bs4 import BeautifulSoup

sleep_time = 3
long_sleep_time = 60 * 3
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


# 第一种代理
def get_proxy():
    return requests.get("http://127.0.0.1:5010/get").json()


proxy = get_proxy().get("proxy")


def getStrHtml(url):
    try:
        global tol_attempts  # 总的尝试次数
        tol_attempts += 1
        # 第二种代理
        # proxypool_url = 'http://127.0.0.1:5555/random'
        # proxy=requests.get(proxypool_url).text.strip()
        # proxies = {'http': 'http://' + proxy}
        # print(proxy)
        # proxy = random.choice(proxy_list)
        # strhtml = requests.get(url, headers=web_header, cookies=cookies)
        strhtml = requests.get(url, headers=web_header, cookies=cookies, proxies={"http": "http://{}".format(proxy)})
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

            # 将页面写入一个.html，用来debug
            # f = open(url[26:36] + ".html", 'w', encoding='utf-8')
            # # str_content = strhtml.text.decode('utf-8')
            # f.write(strhtml.text)
            # f.close()

            return soup
        else:
            print(url)
            print("not movie")
            return 0
    except Exception as e:
        print('[error]', e)
        return -1


def download_one_page(url, ind, file1):
    # print(lineNum)
    # 如果已经被写入，则直接跳过
    # print(lineNum, url[26:36])
    soup = getStrHtml(url)  # 尝试一次
    attempts = 0
    while soup == -1:  # 不成功继续尝试
        attempts += 1
        if attempts == 5:
            break
        time.sleep(sleep_time)
        with eventlet.Timeout(20, False):
            soup = getStrHtml(url)

    if soup == 0:  # 记录不是movie 但是已经爬去取过
        dictionary = {
            'ASIN': url[26:36],
            'image': ' ',
        }

    # 获取到信息了才去选择
    if type(soup) != int:
        # 在这里存储爬取的信息
        dictionary = {
            'ASIN': url[26:36],
            'image': ' ',
            # .......
        }
        # ASIN /Actors /Director /Date First Available信息
        img = str(soup.select('#imgTagWrapperId>img'))
        # print(img)
        img_index = img.find('{')
        img = img[img_index + 2:]
        print(img)
        img_index = img.find('"')
        img = img[:img_index]
        print(img)
        dictionary['image'] = img
        # print(dictionary)
        file1.iloc[ind, 1] = img
        file1.to_csv('image.csv', index=False)
        print(img)
    # 成功率计算部分
    global tol_attempts, success_attempts
    print(tol_attempts, success_attempts, 'suc_rate:', success_attempts / tol_attempts)
    if success_attempts / tol_attempts < 0.30:  # 成功率低于0.2的话休息2分钟
        # time.sleep(long_sleep_time)
        global proxy
        proxy = get_proxy().get("proxy")
        success_attempts = 0  # 重置计数
        tol_attempts = 0
    if tol_attempts > 100:  # 每一百次尝试重置一次计数
        success_attempts = 0
        tol_attempts = 0


def executor_callback(worker):
    logger.info("called worker callback function")
    worker_exception = worker.exception()
    if worker_exception:
        logger.exception("Worker return exception: {}".format(worker_exception))


if __name__ == '__main__':
    # 使用线程池
    file1 = pd.read_csv('image.csv')
    # 创建线程池
    with ThreadPoolExecutor(8) as t:
        for ind, row in file1.iterrows():

            if ind > 26000:
                if row['image'] != 'none':
                    print('already catch')
                    continue
                url = 'https://www.amazon.com/dp/' + row['ASIN']
                future = t.submit(download_one_page, url, ind, file1)
                future.add_done_callback(executor_callback)
            # print(future.exception())
        t.shutdown()

    print("全部下载完毕!")
