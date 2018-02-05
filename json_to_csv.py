#coding: utf-8

import csv
import redis

starts = [2014210001, 2016210001, 2015210001]
ends =   [2014214840, 2016214628, 2015214780]

def grade_data_to_csv(r, writer, grade, num):
    for userId in xrange(starts[num], ends[num]+1):
        json_data = r.hget(grade, str(userId))
        if json_data is None:
            continue
        data_list = eval(json_data)
        if data_list is None:
            continue
        for data in data_list:
            canteen = ""
            if len(data['orgName'].split('/')) > 4:
                canteen = data['orgName'].split('/')[3]
            writer.writerow((userId,
                data['dealDateTime'], data['orgName'], data['outMoney'],
                data['transMoney'], data['userName'], canteen
            ))

def json_to_csv(r):
    with file('ccnu_data.csv', 'wb+') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
        #  学号      交易日期      食堂窗口   账户余额     交易金额       姓名        食堂
        'userId', 'dealDateTime', 'orgName', 'outMoney', 'transMoney', 'userName', 'canteen'
        ])
        # 2014
        # grade_data_to_csv(r, writer, "2014", 0)
        # 2015
        # grade_data_to_csv(r, writer, "2015", 2)
        # 2016
        grade_data_to_csv(r, writer, "2016", 1)

if __name__ == '__main__':
    r = redis.StrictRedis(host='127.0.0.1', port=6380)
    json_to_csv(r)
