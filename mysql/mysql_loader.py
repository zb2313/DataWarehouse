import datetime
import math

import jsonlines
import pymysql

# 数据库连接
dw_mysqldb = pymysql.connect(
    host='139.196.202.57',
    user='dataWarehouseAdm',
    password='dataWarehouse_123',
    database='dataWarehouseDB',
)

# 可以执行sql的光标对象
cursor = dw_mysqldb.cursor()

# 测试用例
# sql = "INSERT INTO div_time(year) VALUES (%s);"
# time_key = 10;
# year = 2021;
#
# try:
#     cursor.execute(sql, [year])
#     dw_mysqldb.commit()
# except Exception as e:
#     print(e)
#     dw_mysqldb.rollback()

MONTHS = ["January", "February", "March", "April", "May",
          "June", "July", "August", "September", "October", "November"
                                                            "December", ]

movies = open("../movies.json", "r", encoding="UTF-8")

count = 1

for item in jsonlines.Reader(movies):
    count += 1
    if count > 10:
        break

    # div_time table
    time_info = item['Date First Available']
    if len(time_info) < 3:
        print("no date error")
    time_key = None
    year = int(time_info[2])
    month = MONTHS.index(time_info[0]) + 1
    day = int(time_info[1])
    quarter = math.ceil(month * 4 / 12)
    weekday = datetime.datetime(year, month, day).isoweekday()
    # print(year, month, day, quarter, weekday)
    # sql语句
    sql_select_time = "SELECT time_key FROM div_time WHERE year = %s AND month = %s AND day = %s;"
    sql_insert_time = "INSERT INTO div_time(year, month, day, quarter, weekday) " \
                      "VALUES (%s, %s, %s, %s, %s);"
    # 执行
    try:
        cursor.execute(sql_select_time, [year, month, day])
        # 没有time记录
        if cursor.rowcount == 0:
            cursor.execute(sql_insert_time, [year, month, day, quarter, weekday])  # 插入
            dw_mysqldb.commit()
            cursor.execute(sql_select_time, [year, month, day])  # 重新查找获取其time_key
            time_key = cursor.fetchone()[0]
            # print(":", time_key)
        elif cursor.rowcount == 1:  # 有一条time记录
            result = cursor.fetchone()
            time_key = result[0]
            # print(")", time_key)
        else:
            print("time error")
    except Exception as e:
        print(e)
        dw_mysqldb.rollback()

    # div_style table
    style_key = None
    style_name = item['style']
    # print(style_name)
    sql_select_unique_style = "SELECT style_key FROM div_style WHERE style_name = %s;"
    sql_insert_style = "INSERT INTO div_style(style_name, style_num)" \
                       "VALUES (%s, %s);"
    sql_update_style = "UPDATE div_style SET style_num = style_num + 1 WHERE style_name = %s;"

    # 执行
    try:
        cursor.execute(sql_select_unique_style, [style_name])

        # print(cursor.rowcount)
        # 不存在style记录
        if cursor.rowcount == 0:
            cursor.execute(sql_insert_style, [style_name, 1])
            dw_mysqldb.commit()
        # 存在记录的话加1
        else:
            cursor.execute(sql_update_style, [style_name])
            dw_mysqldb.commit()
        cursor.execute(sql_select_unique_style, [style_name])  # 查询key
        style_key = cursor.fetchone()[0]
        # print("*", style_key)
    except Exception as e:
        print(e)
        dw_mysqldb.rollback()

    # div_movie_series
    # 暂时还未进行模糊匹配，单纯将电影名当成一个series
    movie_series_id = None
    sql_select_unique_movie_series = "SELECT movie_series_id FROM div_movie_series WHERE series_name = %s;"
    sql_insert_movie_series = "INSERT INTO div_movie_series(series_name, series_num)" \
                              "VALUES (%s, %s);"
    sql_update_movie_series = "UPDATE div_movie_series SET series_num = series_num + 1 WHERE series_name = %s;"
    # 执行
    try:
        cursor.execute(sql_select_unique_movie_series, [item['Title']])
        if cursor.rowcount == 0:
            cursor.execute(sql_insert_movie_series, [item['Title'], 1])
            dw_mysqldb.commit()
        else:
            cursor.execute(sql_update_movie_series, [item['Title']])
            dw_mysqldb.commit()
        cursor.execute(sql_select_unique_movie_series, [item['Title']])
        movie_series_id = cursor.fetchone()[0]
        # print("&", movie_series_id)
    except Exception as e:
        print(e)
        dw_mysqldb.rollback()

    # fact_movie table
    asin = item['ASIN']
    if movie_series_id is None:
        print("movie_series_id error")
    title = item['Title']
    product_version = ','.join(item['version'])
    imdb_score = item['IMDB grade']
    if time_key is None:
        print("time_key error")
    if style_key is None:
        print("style_key error")
    directors_name = ','.join(item['Director'])
    actors_name = ','.join(item['Actors'])

    sql_insert_movie = "INSERT INTO fact_movie(asin, movie_series_id, title, product_version, imdb_score, " \
                       "time_key, style_key, directors_name, actors_name)" \
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    try:
        cursor.execute(sql_insert_movie, [asin, movie_series_id, title, product_version, imdb_score,
                                          time_key, style_key, directors_name, actors_name])
        dw_mysqldb.commit()
    except Exception as e:
        print(e)
        dw_mysqldb.rollback()

    # div_review table
    reviews = item['reviews']
    for review in reviews:
        review_id = review['reviewerid']
        asin
        reviewer_name = review['rewiewername']
        helpful_num = review['helpfulnum']
        score = review['score']
        date = review['date']
        summary = review['summary']
        text = review['text']
        sql_insert_review = "INSERT INTO div_review(review_id, asin, reviewer_name, helpful_num," \
                            "score, date, summary, text)" \
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        try:
            cursor.execute(sql_insert_review, [review_id, asin, reviewer_name,
                                               helpful_num, score, date, summary, text])
        except Exception as e:
            print(e)
            dw_mysqldb.rollback()

    # div_actor, bridge_act
    actors = item['Actors']
    for actor in actors:
        actor_id = None
        sql_select_actor = "SELECT actor_id FROM div_actor WHERE actor_name = %s;"
        sql_insert_actor = "INSERT INTO div_actor(actor_name, movie_name)" \
                           "VALUES (%s, %s);"
        sql_update_actor = "UPDATE div_actor SET movie_name = CONCAT(movie_name, ',', %s) WHERE actor_name = %s;"

        try:
            cursor.execute(sql_select_actor, [actor])
            if cursor.rowcount == 0:
                cursor.execute(sql_insert_actor, [actor, title])
                dw_mysqldb.commit()
            else:
                cursor.execute(sql_update_actor, [title, actor])
                dw_mysqldb.commit()
            cursor.execute(sql_select_actor, [actor])
            actor_id = cursor.fetchone()[0]
            print("actor_id", actor_id)
        except Exception as e:
            print(e)
            dw_mysqldb.rollback()

        # 存入bridge联系表
        sql_insert_bridge_act = "INSERT INTO bridge_act(asin, actor_id)" \
                                "VALUES (%s, %s);"
        try:
            cursor.execute(sql_insert_bridge_act, [asin, actor_id])
            dw_mysqldb.commit()
        except Exception as e:
            print(e)
            dw_mysqldb.rollback()

    # div_director, bridge_direct
    directors = item['Director']
    for director in directors:
        director_id = None
        sql_select_director = "SELECT director_id FROM div_director WHERE director_name = %s;"
        sql_insert_director = "INSERT INTO div_director(director_name, movie_name)" \
                              "VALUES (%s, %s);"
        sql_update_director = "UPDATE div_director SET movie_name = CONCAT(movie_name, ',', %s) WHERE director_name = %s;"

        try:
            cursor.execute(sql_select_director, [director])
            if cursor.rowcount == 0:
                cursor.execute(sql_insert_director, [director, title])
                dw_mysqldb.commit()
            else:
                cursor.execute(sql_update_director, [title, director])
                dw_mysqldb.commit()
            cursor.execute(sql_select_director, [director])
            director_id = cursor.fetchone()[0]
            print("director_id", director_id)
        except Exception as e:
            print(e)
            dw_mysqldb.rollback()

        # 存入bridge 联系表
        sql_insert_bridge_direct = "INSERT INTO bridge_direct(asin, director_id)" \
                                   "VALUES (%s, %s);"
        try:
            cursor.execute(sql_insert_bridge_direct, [asin, director_id])
            dw_mysqldb.commit()
        except Exception as e:
            print(e)
            dw_mysqldb.rollback()


cursor.close()
dw_mysqldb.close()
