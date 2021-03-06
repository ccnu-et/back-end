import os
import jieba
import asyncio
import aioredis
import pandas as pd
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

executor = ThreadPoolExecutor(max_workers=15)

def ccnu_cache(f):
    @wraps(f)
    async def decorator(*args, **kwargs):
        rds = args[0]
        json_data = await f(*args, **kwargs)
        # cache
        await rds.set('api_' + f.__name__, str(json_data))
        return await f(*args, **kwargs)
    return decorator

async def init_dataset():
    # 初始化数据集
    ccnu = pd.read_csv('ccnu_data.csv')
    # drop_indexes = ccnu[
    #     #--------------------------------------------------
    #     (   ccnu['orgName'].str.contains("超市"))         |
    #     (   ccnu['orgName'].str.contains("保卫处"))       |
    #     (   ccnu['orgName'].str.contains("后勤处"))       |
    #     (   ccnu['orgName'].str.contains("信息化办公室")) |
    #     (   ccnu['orgName']      ==      '华中师范大学')  |
    #     (   ccnu['transMoney'] <= 0                     ) |
    #     (   ccnu['transMoney'] >= 90                    )#|
    #     #--------------------------------------------------
    # ].index
    # ccnu = ccnu.drop(drop_indexes)
    ## 1. 删除错误数据
    canteen = ccnu[ ccnu['orgName'].str.contains("饮食中心") ]
    canteen = canteen.drop(canteen[
        ( canteen['orgName'].str.contains("超市") ) |
        ( canteen['transMoney'] <= 0 ) |
        ( canteen['transMoney'] >= 90 )
    ].index)
    ## 2. 处理食堂窗口名称
    canteen['orgName'] = canteen['orgName'].apply(
        lambda x: x.split('/')[3] + '&' + x.split('/')[-1]
    )
    ## 3. 食堂窗口名称分词
    orgName = canteen.groupby('orgName').size().reset_index(name="value") # value; sort
    orgName['words_in_orgName'] = orgName['orgName'].apply(
        lambda x: ' '.join(jieba.cut(x.split('&')[1]))
    )
    ## 4. 计算食堂窗口名称tf-idf矩阵
    tfidf = TfidfVectorizer()
    orgName['words_in_orgName'] = orgName['words_in_orgName'].fillna('')
    tfidf_matrix = tfidf.fit_transform(orgName['words_in_orgName'])
    ## 5. 计算各食堂窗口的余弦相似度, 用于推荐
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)
    # app.config.CCNU = canteen
    # app.config.COS = cosine_sim
    return canteen, cosine_sim

@ccnu_cache
async def meta_data(rds, canteen):
    data_len = len(canteen)
    return data_len

@ccnu_cache
async def max_canteen(rds, canteen):
    # 消费次数最多的食堂
    # + 饼图
    canteen_count = canteen.groupby('canteen').size().reset_index(name="value")
    canteen_count.columns = ['name', 'value']
    data_dict = eval(canteen_count.to_json(orient='index'))
    legend_dict = eval(canteen_count['name'].to_json())
    data = list(data_dict.values())
    legend = list(legend_dict.values())
    return {
        'data': data, 'legend': legend
    }

@ccnu_cache
async def max_window(rds, canteen):
    # 刷卡次数前6的食堂窗口
    loop = asyncio.get_event_loop()
    breakfast = ['06:30:00', '10:30:00']
    lunch = ['10:40:00', '14:00:00']
    dinner = ['17:00:00', '21:30:00']

    orgsb_list = await loop.run_in_executor(
        executor, handle_org, canteen, breakfast
    )
    orgsl_list = await loop.run_in_executor(
        executor, handle_org, canteen, lunch,
    )
    orgsd_list = await loop.run_in_executor(
        executor, handle_org, canteen, dinner,
    )

    return {
        'breakfast': {
            'xAxis': [item['orgName'] for item in orgsb_list],
            'data': [item['value'] for item in orgsb_list] 
        },
        'lunch': {
            'xAxis': [item['orgName'] for item in orgsl_list],
            'data': [item['value'] for item in orgsl_list] 
        },
        'dinner': {
            'xAxis': [item['orgName'] for item in orgsd_list],
            'data': [item['value'] for item in orgsd_list] 
        }
    }

@ccnu_cache
async def deal_data(rds, canteen):
    # 华师各食堂消费水平
    loop = asyncio.get_event_loop()
    avg_data = canteen["transMoney"].mean().round(2),
    avg = avg_data[0]
    low = []; high = []
    for name in ["学子餐厅", "东一餐厅新", "东二餐厅", "博雅园餐厅", "学子中西餐厅",
                 "桂香园餐厅新", "沁园春餐厅", "北区教工餐厅", "南湖校区餐厅"]:
        await loop.run_in_executor(executor,
                handle_trans, canteen, name, low, high, avg)
    return {
        'avg' : avg_data[0], 'low': low, 'high': high
    }

@ccnu_cache
async def day_canteen(rds, canteen):
    # 华师主要食堂每日刷卡量对比
    loop = asyncio.get_event_loop()
    canteen['dealDay'] = pd.to_datetime(canteen['dealDateTime'])
    canteen['day'] = canteen['dealDay'].apply(lambda x: x.dayofweek)
    canteens = ["东一餐厅(旧201508)", "东一餐厅新", "东二餐厅", "北区教工餐厅",
            "南湖校区餐厅", "博雅园餐厅", "学子中西餐厅", "学子餐厅",
            "桂香园餐厅新", "沁园春餐厅"]
    json_list = []
    for name in canteens:
        json_dict = await loop.run_in_executor(executor, handle_day, canteen, name)
        json_list.append(json_dict)
    return json_list

# cpu intensive tasks
## running in thread pool
def handle_org(canteen, time):
    canteen_x = canteen[
        (canteen['dealDateTime'].str.split().str[1] >= time[0]) &
        (canteen['dealDateTime'].str.split().str[1] <= time[-1])
    ]
    orgsx = canteen_x.groupby('orgName').size().reset_index(name="value")
    orgsx_dict = eval(orgsx.sort_values(by=['value']).tail(6).to_json(orient='index'))
    return orgsx_dict.values()

def handle_trans(canteen, name, low, high, avg):
    avgd = canteen[ (canteen["canteen"]==name) ]["transMoney"].mean().round(2)
    maxd = canteen[ (canteen["canteen"]==name) ]["transMoney"].max()
    if avgd < avg:
        low.append([maxd, avgd, name])
    else:
        high.append([maxd, avgd, name])

def handle_day(canteen, name):
    # 'dealDateTime'
    day_count = canteen[ canteen['canteen']==name ].groupby('day').size().reset_index()
    data_dict = eval(day_count.to_json())
    data_list = list(data_dict.values())
    return {
        'name': name, 'type': 'line', 'stack': '刷卡量',
        'data': list(list(data_list)[-1].values())
    }
