import argparse
from ETL.utils.ExtractAsin import extractAsin
from ETL.extract import TextExtract, ImageExtract
from ETL.utils.RecordAsin import recordAsin
from ETL.utils.JsonToCsv import createImageCSV
from ETL.transform.TransformMovie import transformMovie
from ETL.transform.SentiScore import getSentiScore
from ETL.load.neo4j import Neo4jLoader
from ETL.load.mysql import ImageLoader,TextLoader



if __name__ == '__main__':
    extractAsin()  # 提取asin asin唯一表示一件amazon商品
    for i in range(0, 10):  # 文本爬取 一轮爬不完 爬取10轮
        TextExtract.run()
        recordAsin()  # 标记已经爬过的asin

    createImageCSV()  # 创建Image.csv用于存储电影图片链接
    ImageExtract.run()  # 爬取电影图片

    transformMovie()  # 处理爬到的数据 包括统一日期格式、电影评分
    getSentiScore()  # 情感分析获得用户评价的感情评分，分值越高，越积极

    Neo4jLoader.run()  # 加载到图数据库

    ImageLoader.loadImage()  # 存储图片到mysql
    TextLoader.loadText()  # 存储数据到mysql





