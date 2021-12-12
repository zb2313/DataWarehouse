import pandas as pd
import pymysql

# 数据库连接
dw_mysqldb = pymysql.connect(
    host='139.196.202.57',
    # host='127.0.0.1',
    user='dataWarehouseAdm',
    password='dataWarehouse_123',
    database='dataWarehouseDB',
)

# 可以执行sql的光标对象
cursor = dw_mysqldb.cursor()
df = pd.read_csv("../image.csv")

for i in range(len(df)):
    asin = df['ASIN'][i]
    url = df['image'][i]
    print(asin, url)

    if df['image'][i] == 'none':
        print("no image url")
        continue

    sql_select_asin = "SELECT asin FROM fact_movie WHERE asin = %s;"
    sql_update_image = "UPDATE fact_movie SET movie_pic = %s WHERE asin = %s;"
    try:
        cursor.execute(sql_select_asin, asin)
        if cursor.rowcount == 0:
            print("no this movie")
            continue
        else:
            cursor.execute(sql_update_image, [url, asin])
            dw_mysqldb.commit()  # 一定不能忘记提交
    except Exception as e:
        print(e)
        dw_mysqldb.rollback()
