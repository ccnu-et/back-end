import pandas as pd
from sanic import Sanic
from sanic.response import json
from sanic_cors import CORS, cross_origin

app = Sanic()
CORS(app)
app.config.CCNU = pd.read_csv('ccnu_data.csv')

@app.listener('before_server_start')
async def drop_bad_data(app, loop):
    # 删除错误数据
    ccnu = app.config.CCNU
    drop_indexes = ccnu[
        #--------------------------------------------------
        (   ccnu['orgName'].str.contains("超市"))         |
        (   ccnu['orgName'].str.contains("保卫处"))       |
        (   ccnu['orgName'].str.contains("后勤处"))       |
        (   ccnu['orgName'].str.contains("信息化办公室")) |
        (   ccnu['orgName']      ==      '华中师范大学')# |
        #--------------------------------------------------
    ].index
    ccnu = ccnu.drop(drop_indexes)
    app.config.CCNU = ccnu

@app.route('/api/')
async def meta_data(request):
    # meta data
    ccnu = request.app.config.CCNU
    # print("------------- Meta Data --------------")
    # print(ccnu.info())
    # print(ccnu.describe())
    # print("-------------- 上半年 ----------------")
    # print(ccnu[ ccnu['dealDateTime'] <= '2017-06-15 00:00:00' ].info())
    # print("-------------- 下半年 ----------------")
    # print(ccnu[ ccnu['dealDateTime'] > '2017-06-15 00:00:00' ].info())
    return json({"meta": ccnu.info()})

@app.route('/api/max_canteen/')
async def max_canteen_1(request):
    # 消费次数最多的食堂
    # + 柱状图, 饼图
    # + 上下半年消费对比
    # print("--------------- 消费前10的食堂窗口消费统计 ---------------")
    # canteen = ccnu[ ccnu['orgName'].str.contains("饮食中心") ]
    # window_count = canteen.groupby('orgName').size().reset_index(name='counts')
    # print(window_count.sort_values(by=['counts']).tail(10))
    # print("-------------------- 主要食堂消费统计 --------------------")
    ccnu = request.app.config.CCNU
    canteen = ccnu[ ccnu['orgName'].str.contains("饮食中心") ]
    canteen_count = canteen.groupby('canteen').size().reset_index(name="value")
    canteen_count.columns = ['name', 'value']
    data_dict = eval(canteen_count.to_json(orient='index'))
    legend_dict = eval(canteen_count['name'].to_json())
    return json({
        'data': data_dict.values(),
        'legend': legend_dict.values()
    })
    
if __name__ == '__main__':
    # from aoiklivereload import LiveReloader
    # reloader = LiveReloader()
    # reloader.start_watcher_thread()
    app.run(host='0.0.0.0', port=3000, debug=True)
