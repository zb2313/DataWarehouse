# DataWarehouse



## ETL

### 概览

​	项目数据来源：使用[https://nijianmo.github.io/amazon/index.html](https://nijianmo.github.io/amazon/index.html)的 “Small” subsets for experimentation 中的 Movies and TV 5-core 一共3,410,019条用户商品评论信息，利用数据中的“asin”字段对亚马逊页面进行信息爬取。

​	在对其中的“asin”字段进行去重处理后，总共得到了60172条不重复的asin数据，保存为csv文件；通过亚马逊商品页面中的imdb标签对商品进行区分，判断其是否是电影；采用IP代理池和python线程池来提高爬取的效率；数据存储采用三种不同数据库：MySQL、Neo4j、Hive，综合对比不同数据库的查询性能差异

>### ETL使用教程
>
>⭕️⭕️⭕️⭕️⭕️
>
>
>
>



### Extract

+ #### 爬虫

  + 首先对源数据进行处理之后，得到其asin字段的csv文件，利用其asin字段构造一系列Amazon商品页面的url 

    ```python
    asin = "157252765X"
    url = 'https://www.amazon.com/dp/ + asin
    ```

  + 使用request库发送请求

    ```python
    strhtml = requests.get(url, headers=web_header, cookies=cookies, proxies={"http": "http://{}".format(proxy)})
    ```
    需要注意的是，爬取亚马逊的页面必须要包括基本的请求头以及cookies等基本内容，否则无法大量爬取
    
  + 使用lxml和beautifulsoup库解析网页，通过selector获取相关字段
  
    ```python
    soup = BeautifulSoup(strhtml.text, 'lxml')
    movie_title = str(soup.select('title')[0].getText())
    ```
    
  + 使用python线程池
    
    ```python
    with ThreadPoolExecutor(8) as t:
        for item in reader:
            if reader.line_num < 2:
                continue
            if reader.line_num > 15000:
                break
            url = 'https://www.amazon.com/dp/' + item[0]
            future = t.submit(download_one_page, url, reader.line_num)
            future.add_done_callback(executor_callback)
            t.shutdown()
    ```
    
    相比python多线程，线程池可以提供更加方便的使用策略，线程池中的线程是重复使用的，在其分配的任务被完成完成后，它会继续被分配任务；而多线程需要重复启动线程，这样会浪费系统资源
    
    需要注意的是，在线程中的错误是不会被影响到主线程，也就是说，子线程出现错误后会被分配下一个任务，而主线程不会终止，自然也没有错误输出
    
  +  使用IP代理池
    
    在不使用IP代理池时，爬取过程只能维持短时间（20～30min）的高成功率（85%以上），之后单次爬取的成功率会降低到（10%）以下，但当切换IP之后（使用手机热点等），其爬取成功率会恢复，但在上述时间之后，爬取成功率仍然会大幅降低。同一个IP需要暂停一段时间才能重新恢复到初始状态，但具体时间无法得知，（上限可知：超过12h）
    
    所以需要进行IP代理来提高长时间段内的爬取成功率：
    
    [proxy_pool](https://github.com/jhao104/proxy_pool)
    
    [ProxyPool](https://github.com/Python3WebSpider/ProxyPool)
    
    使用了以上两个代理池进行了可用代理IP的获取，具体使用过程请点击链接查看
    
    但上述IP池的数量并不是非常充足，因此采用了“先使用一个ip，当其爬取成功率降低到一定程度时，换用另一个ip的策略”
    
    
  
+ #### 数据暂存策略

  由于需要爬取的页面众多，不能一次性爬取完毕，所以在asin的csv文件中增加一列，用于记录已经爬取过的，或是已经确认不是movie的asin，这样可以多次关停程序，间断的爬取数据

### Transform

+ #### 电影系列判定

  采用Levenshtein距离算法，计算两个电影名称的相似度，同时根据实际情况设置不同情况的权重，比如减少长度相差过大的电影名称的相似度，降低某些无意义的单词（“The”， “At”）的权重，最后选取某一个相似度的界限，当相似度超过该值时，视两部电影为一个系列

+ #### 评论情感分析

  ⭕️⭕️⭕️⭕️⭕️⭕️⭕️

+ #### 其他

  ⭕️⭕️⭕️⭕️⭕️

### Load

+ #### MySQL

  存储数据库安装在Centos8.2的阿里云服务器上，因为存储时需要计算电影系列，时间复杂度是$O(n^2)$，因此存储脚本选择在服务器上挂起执行，节约本机资源

+ #### Neo4j

  ⭕️⭕️⭕️⭕️⭕️

+ #### Hive

  ⭕️⭕️⭕️⭕️⭕️

## 数据存储设计说明



### 关系型存储



### 分布式存储



### 图数据存储





## 项目报告



### 数据查询程序



#### 前端程序



#### 后端程序



### 存储优化分析



### 数据质量分析



### 数据血缘分析