import jsonlines
import json

file_path = 'E:\\movies.json'
file = open('E:\\movies.json', 'r', encoding='utf-8')
file2 = jsonlines.open('E:\\movies2.json', 'a')
for line in file.readlines():
    dic = json.loads(line)
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
