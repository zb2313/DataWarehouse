# 作者：韩乃超
# 日期：2021年10月31日
from selenium import webdriver
from selenium.webdriver import ChromeOptions
import csv
import csv
import time

import eventlet
import requests  # 导入requests包
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


class data():
    def __init__(self, url):
        self.url = url
        opt = ChromeOptions()
        opt.add_experimental_option('excludeSwitches', ['enable-automation'])  # 开启实验性功能
        opt.add_experimental_option('prefs', {'profile.managed_default_content_settings.images': 2})  # 禁止图片加载
        opt.add_argument('--headless')
        opt.add_argument('--disable-gpu')  # 设置无头浏览器
        self.driver = webdriver.Chrome(options=opt)
        # 0767020294
        self.driver.get(self.url)

        self.Parser_Product_Data()
        self.driver.quit()

    def Parser_Product_Data(self):
        dic = {}
        try:
            title = self.driver.find_element_by_xpath('//*[@id="productTitle"]').text

            dic["title"] = "".join(title).replace("\n", "").strip()
        except:
            dic["title"] = ""
        li_list = self.driver.find_elements_by_xpath('//*[@id="detailBullets_feature_div"]/ul/li')
        for li in li_list:
            temp = li.find_element_by_xpath('./span/span[1]').text

            if (temp == '导演 :'):
                try:
                    author = li.find_element_by_xpath('./span/span[2]').text
                    dic["author"] = "".join(author).replace("\n", "").strip()
                except:
                    dic["author"] = ""
            if (temp == '演员 :'):
                try:
                    actors = li.find_element_by_xpath('./span/span[2]').text

                    dic["actors"] = "".join(actors).replace("\n", "").strip()
                except:
                    dic["actors"] = ""

        with open(".//data.csv", "a", encoding="utf-8", newline='') as f:
            writer = csv.DictWriter(f, dic.keys())
            writer.writerow(dic)

        self.driver.close()


if __name__ == '__main__':
    # 创建csv文件
    dic = {'0': '电影名称', '1': '导演', '2': '演员'}

    with open(".//data.csv", "a", encoding="utf-8", newline='') as f:
        writer = csv.DictWriter(f, dic.keys())
        writer.writerow(dic)

    asinFile = open("asin.csv")
    reader = csv.reader(asinFile)
    # 创建线程池
    with ThreadPoolExecutor(20) as t:
        for item in reader:
            if reader.line_num == 1:
                continue
            if reader.line_num == 100:
                break
            url = 'https://www.amazon.com/dp/' + item[1]
            t.submit(data, url)
    asinFile.close()
    print("全部下载完毕!")
