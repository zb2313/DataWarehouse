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
    year = int(time_info[2])
    month = MONTHS.index(time_info[0]) + 1
    day = int(time_info[1])
    quarter = math.ceil(month * 4 / 12)
    weekday = datetime.datetime(year, month, day).isoweekday()
    # print(year, month, day, quarter, weekday)
    # sql语句
    sql_time_info = "INSERT INTO div_time(year, month, day, quarter, weekday) " \
                    "VALUES (%s, %s, %s, %s, %s);"
    # 执行
    # try:
    #     cursor.execute(sql_time_info, [year, month, day, quarter, weekday])
    #     dw_mysqldb.commit()
    # except Exception as e:
    #     print(e)
    #     dw_mysqldb.rollback()

    # div_style table
    style_name = item['style']
    sql_select_unique_style = "SELECT style_name" \
                              "FROM div_style" \
                              "WHERE style_name = %s;"

    try:
        cursor.execute(sql_select_unique_style, [style_name])
        dw_mysqldb.commit()
    except Exception as e:
        print(e)
        dw_mysqldb.rollback()


cursor.close()
dw_mysqldb.close()
