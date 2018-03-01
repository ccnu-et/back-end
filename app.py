import jieba
import pandas as pd
from functools import wraps
from sanic import Sanic
from sanic.response import json
from sanic_cors import CORS, cross_origin
from concurrent.futures import ThreadPoolExecutor
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

app = Sanic()
CORS(app)
app.config.CCNU = pd.read_csv('ccnu_data.csv')
executor = ThreadPoolExecutor(max_workers=15)

def ccnu_loop(f):
    @wraps(f)
    async def decorator(*args, **kwargs):
        ccnu = args[0].app.config.CCNU
        cos = args[0].app.config.COS
        loop = args[0].app.loop
        canteen = ccnu.copy()
        return await f(*args, canteen, cos, loop, **kwargs)
    return decorator

@app.listener('before_server_start')
async def init_dataset(app, loop):
    # 初始化数据集
    ccnu = app.config.CCNU
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
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)
    app.config.CCNU = canteen
    app.config.COS = cosine_sim

@app.route('/api/')
@ccnu_loop
async def meta_data(request, canteen, orgName, loop):
    # meta data
    data_len = len(canteen)
    return json({"meta": { 'data_len': data_len }})

from data_analysis import *
from recommender import *
