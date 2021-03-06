# 作者：韩乃超
# 日期：2021年10月29日
######
#
#
#   使用方法：先运行get_asins.py生成爬取记录表 newAsin.csv
#   接着运行本文件
#   运行后使用recordAsin.py记录下爬取的movie asin号（记录在newAsin.csv中）

import csv
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor

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


def download_one_page(url, lineNum):
    # print(lineNum)
    jsonwriter = jsonlines.open("movie.json", "a")
    # 如果已经被写入，则直接跳过
    # print(lineNum, url[26:36])
    if df.loc[lineNum - 2, 'isGot'] == 1:
        return

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
            'isMovie': False,
        }
        jsonwriter.write(dictionary)
        jsonwriter.close()

    # 获取到信息了才去选择
    if type(soup) != int:
        # 在这里存储爬取的信息
        dictionary = {
            'ASIN': '',
            'Title': ' ',
            'Actors': [],
            'Director': [],
            'IMDB grade': '',
            'Date First Available': '',
            'style': '',
            'version': [],
            'reviews': [],
            # .......
        }
        # ASIN /Actors /Director /Date First Available信息
        name = str(soup.select("#productTitle")).replace("\n", "")[71:].replace("</span>]", "").replace("VHS", "")
        dictionary['Title'] = name

        imdb_grade = float(str(soup.select("#imdbInfo_feature_div>span>strong"))[9:12])
        dictionary['IMDB grade'] = imdb_grade

        date_soup = soup.select("#declarative_ .dp-title-col .title-text>span")
        if len(date_soup) >= 3:
            date = str(soup.select("#declarative_ .dp-title-col .title-text>span")[2])[40:-7]
            dictionary['Date First Available'] = date

        person_careers = soup.select("#bylineInfo > span > span > span")
        person_names = soup.select("#bylineInfo > span > a.a-link-normal")

        for index in range(len(person_careers)):
            if "Actor" in person_careers[index].get_text():
                dictionary['Actors'].append(person_names[index].get_text())
            if "Director" in person_careers[index].get_text():
                dictionary['Director'].append(person_names[index].get_text())

        dictionary['ASIN'] = url[26:36]

        # style（风格）
        styledata = soup.select("#wayfinding-breadcrumbs_feature_div > ul > li > span > a")
        # print(styledata)
        lenth = len(styledata)
        # print(lenth)
        # tempstr = styledata[lenth - 1].get_text()
        # print(tempstr.strip())
        dictionary['style'] = styledata[lenth - 1].get_text().strip()

        # 格式
        versiondata = soup.select("#tmmSwatches > ul > li > span > span > span > a > span:nth-child(1)")
        for item in versiondata:
            # print(item.get_text())
            dictionary['version'].append(item.get_text())

        # 评论相关信息
        reviewdata = soup.select("#cm-cr-dp-review-list > div")
        reviews = []

        count = 0
        for item in reviewdata:  # 循环读取评论
            count += 1
            if count > 5:
                break
            reviewerid = item['id']
            # print(item['id']) #id

            reviewername = item.select("div > a > div.a-profile-content > span")[0].get_text()
            # print(item.select("div > a > div.a-profile-content > span")[0].get_text()) # 用户名

            helpfulstr = item.select(".a-size-base.a-color-tertiary.cr-vote-text")
            if len(helpfulstr) == 0:  # 有的评论下面没有有帮助text
                numhelpful = 0
            else:
                helpfulstr = helpfulstr[0].get_text()
                numhelpful = re.findall("\d+", helpfulstr)  # 当只有一个人时 显示的数字是“one”
                if len(numhelpful) == 0:
                    numhelpful = 1
                else:
                    numhelpful = numhelpful[0]

            # print(helpfulstr)
            # print(re.findall("\d+", helpfulstr)[0]) # 有帮助人数

            starclass = item.select(".a-icon.a-icon-star")[0]['class'][2]
            starnum = re.findall("\d+", starclass)[0]
            # print(re.findall("\d+", starclass)[0]) # 星级

            dateinfo = item.select(".a-color-secondary.review-date")[0].get_text()
            # print(dateinfo) # 时间

            summary = item.select(".a-color-base.review-title-content.a-text-bold > span")[0].get_text()
            # print(summary) # 总结

            text = item.select(".a-expander-partial-collapse-content > span")[0].get_text()
            # print(text) # 评论text

            reviewdic = {
                'reviewerid': reviewerid,
                'rewiewername': reviewername,
                'helpfulnum': numhelpful,
                'score': starnum,
                'date': dateinfo,
                'summary': summary,
                'text': text
            }
            # print(reviewdic)
            reviews.append(reviewdic)

            # dictionary['review'].append(reviewdic)
        # print(reviews)
        dictionary['reviews'] = reviews
        # print(dictionary)
        jsonwriter.write(dictionary)
        jsonwriter.close()

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
    asinFile = open("newAsin.csv")
    reader = csv.reader(asinFile)
    # 创建线程池
    with ThreadPoolExecutor(8) as t:
        for item in reader:
            if reader.line_num < 2:
                continue
            if reader.line_num > 15000:
                break
            url = 'https://www.amazon.com/dp/' + item[0]
            future = t.submit(download_one_page, url, reader.line_num)
            future.add_done_callback(executor_callback)
            # print(future.exception())
        t.shutdown()
    asinFile.close()

    print("全部下载完毕!")

