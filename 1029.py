import csv
import time
import pandas as pd
import eventlet
import requests  # 导入requests包
from bs4 import BeautifulSoup
from threading import Thread

sleep_time = 0.5
threads_num = 1
base_url = 'https://www.amazon.com/dp/'
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


def thread_loop(t_id, t_max):
    df = pd.read_csv('asin.csv')['asin']
    p_id_array = df.values
    for index, p_id in enumerate(p_id_array):
        if (index + 1) % t_max == t_id:
            attempts, success = 0, False
            while attempts < 5 and not success:
                try:
                    while True:
                        url = base_url + p_id
                        result = requests.get(url)
                        result = result.text.encode('gbk', 'ignore').decode('gbk')
                        soup = BeautifulSoup(result, 'lxml')
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
                    attempts += 1
                    time.sleep(sleep_time)
                    if attempts == 3:
                        break
if __name__ == '__main__':
    for t_id in range(threads_num):
        print('[Thread]:', t_id, ' begins')
        t = Thread(target=thread_loop, args=(t_id,threads_num,))
        t.start()