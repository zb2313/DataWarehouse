# 作者：韩乃超
# 日期：2021年10月29日


import csv
import time


import eventlet
import requests  # 导入requests包
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor



sleep_time = 1
cookies = {}
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

        strhtml = requests.get(url, headers=web_header, cookies=cookies)
        # print(strhtml.status_code)
        soup = BeautifulSoup(strhtml.text, 'lxml')
        movie_title = str(soup.select('title')[0].getText())

        if movie_title == "Amazon.com":
            return -1

        print(movie_title)

        data = soup.select('#imdbInfo_feature_div > span > i')
        emptyData = soup.select('#cannotbefound')  # 创造一个空的data

        if data != emptyData:
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




def download_one_page(url):


    soup = -1

    soup = getStrHtml(url)
    attemps = 0

    while (soup == -1):
        attemps += 1
        time.sleep(sleep_time)
        with eventlet.Timeout(10, False):
            soup = getStrHtml(url)




if __name__ == '__main__':

    asinFile = open("asin.csv")
    reader = csv.reader(asinFile)
    # 创建线程池
    with ThreadPoolExecutor(50) as t:
        for item in reader:
            if reader.line_num == 1:
                continue
            url = 'https://www.amazon.com/dp/' + item[1]
            t.submit(download_one_page, url)
    asinFile.close()
    print("全部下载完毕!")
