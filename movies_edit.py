import jsonlines
import json

file = open('E:\\movies.json', 'r', encoding='utf-8')
file2 = jsonlines.open('E:\\movies2.json', 'a')
for line in file.readlines():
    dic = json.loads(line)
    # 评论得分处理 存在没评论的情况
    if len(dic['reviews']) >= 1:
        t = 0
        for item in dic['reviews']:
            t += float(item['score'])
        average = round(t / len(dic['reviews']), 1)
        print(average)
        dic['ReviewPoint'] = average
    # 时间处理
    dic_split = str(dic['Date First Available']).replace(' ', ',').split(',')
    if len(dic_split) != 3 and len(dic['reviews']) >= 1:  # 取评论
        print(dic['reviews'][0]['date'])
        new_str = str(dic['reviews'][0]['date']).replace(' ', ',').split(',')[-4:]
        del new_str[2]
        print(new_str)
        dic['Date First Available'] = new_str
        file2.write(dic)
        continue
    print(dic['Date First Available'])
    date = str(dic['Date First Available']).replace(',', '').split(' ')
    print(date)
    dic['Date First Available'] = date

    file2.write(dic)

file.close()
file2.close()

# 日期转换
